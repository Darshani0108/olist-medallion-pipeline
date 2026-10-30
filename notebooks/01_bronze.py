# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze layer: raw ingestion
# MAGIC Loads every CSV **as-is** (all columns as strings, no cleaning) into Delta tables and adds
# MAGIC lineage columns `_ingested_at` and `_source_file`. Row counts are logged to `ingestion_log`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

log_rows = []
for name, file_name in RAW_FILES.items():
    reader = spark.read.option("header", True).option("inferSchema", False)
    if name == "reviews":   # review comments contain quotes and line breaks
        reader = reader.option("multiLine", True).option("escape", '"')

    df = (reader.csv(f"{RAW_PATH}/{file_name}")
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.lit(file_name)))
    n = write_delta(df, BRONZE, name)
    log_rows.append((name, file_name, n))

# COMMAND ----------

log_df = (spark.createDataFrame(log_rows, "table_name string, source_file string, rows_loaded long")
          .withColumn("run_ts", F.current_timestamp()))
write_delta(log_df, BRONZE, "ingestion_log", mode="append")
display(log_df)
