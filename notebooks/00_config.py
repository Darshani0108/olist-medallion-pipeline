# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Config and shared helpers
# MAGIC Imported by the other notebooks with `%run ./00_config`. **Edit the CONFIG cell first.**

# COMMAND ----------

from pyspark.sql import functions as F

# ============================== CONFIG (EDIT) ================================
# Folder that holds the 9 Olist CSV files. Examples:
#   Unity Catalog volume : "/Volumes/workspace/default/olist_raw"
#   Legacy DBFS          : "dbfs:/FileStore/olist"
RAW_PATH = "/Volumes/workspace/default/olist_raw"

CATALOG = None            # e.g. "workspace"; None = use the current default catalog
BRONZE, SILVER, GOLD = "olist_bronze", "olist_silver", "olist_gold"

RAW_FILES = {             # table name -> CSV file name
    "customers":            "olist_customers_dataset.csv",
    "orders":               "olist_orders_dataset.csv",
    "order_items":          "olist_order_items_dataset.csv",
    "payments":             "olist_order_payments_dataset.csv",
    "reviews":              "olist_order_reviews_dataset.csv",
    "products":             "olist_products_dataset.csv",
    "sellers":              "olist_sellers_dataset.csv",
    "geolocation":          "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}
# =============================================================================

# Malformed values should become NULL and be caught by the data quality rules
# instead of crashing the job (newer runtimes enable ANSI mode by default).
try:
    spark.conf.set("spark.sql.ansi.enabled", "false")
except Exception as e:
    print("Could not change ANSI setting:", e)

if CATALOG:
    spark.sql(f"USE CATALOG {CATALOG}")
for _schema in (BRONZE, SILVER, GOLD):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_schema}")


def write_delta(df, schema, table, partition_by=None, mode="overwrite"):
    """Write a DataFrame as a managed Delta table and return its row count."""
    writer = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(f"{schema}.{table}")
    n = spark.table(f"{schema}.{table}").count()
    print(f"{schema}.{table}: {n:,} rows")
    return n


# ------------------------- data quality framework ----------------------------
DQ_LOG = []   # (table_name, check_name, rows_checked, rows_failed)


def log_dq(table, check, checked, failed):
    DQ_LOG.append((table, check, int(checked), int(failed)))


def apply_rules(df, rules):
    """rules = {name: Column that is TRUE for a valid row}. Returns (valid, rejected).
    Rejected rows keep a `_failed_rules` array naming every rule they broke."""
    flags = [F.when(~F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in rules.items()]
    checked = df.withColumn("_failed_rules", F.filter(F.array(*flags), lambda x: x.isNotNull()))
    valid = checked.filter(F.size("_failed_rules") == 0).drop("_failed_rules")
    rejected = checked.filter(F.size("_failed_rules") > 0)
    return valid, rejected


def run_quality(df, table, rules, key=None):
    """Deduplicate on `key`, apply rules, log every check, quarantine rejected rows."""
    if key:
        total = df.count()
        df = df.dropDuplicates(key)
        log_dq(table, "duplicate_keys_removed", total, total - df.count())

    valid, rejected = apply_rules(df, rules)
    n_checked = df.count()
    failed = {r["rule"]: r["count"] for r in
              rejected.select(F.explode("_failed_rules").alias("rule")).groupBy("rule").count().collect()}
    for name in rules:
        log_dq(table, name, n_checked, failed.get(name, 0))

    quarantine = rejected.withColumn("_failed_rules", F.concat_ws(",", F.col("_failed_rules")))
    write_delta(quarantine, SILVER, f"{table}_rejected")
    return valid


def enforce_fk(df, ref, key, table, check):
    """Keep only rows whose `key` exists in `ref` (referential integrity) and log the orphans."""
    before = df.count()
    kept = df.join(ref.select(key).distinct(), key, "left_semi")
    log_dq(table, check, before, before - kept.count())
    return kept


def flush_dq():
    """Append this run's quality results to olist_silver.dq_log."""
    if not DQ_LOG:
        return
    schema = "table_name string, check_name string, rows_checked long, rows_failed long"
    log_df = spark.createDataFrame(DQ_LOG, schema).withColumn("run_ts", F.current_timestamp())
    write_delta(log_df, SILVER, "dq_log", mode="append")
    DQ_LOG.clear()
