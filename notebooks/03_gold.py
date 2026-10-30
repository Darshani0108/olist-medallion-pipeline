# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold layer: star schema and business aggregates
# MAGIC **Fact:** `fact_order_items` (partitioned by year and month), `fact_payments`
# MAGIC **Dimensions:** `dim_date`, `dim_customer`, `dim_product`, `dim_seller`
# MAGIC **Aggregates:** `agg_monthly_revenue`, `agg_category_performance`, `agg_state_delivery`

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

orders    = spark.table(f"{SILVER}.orders")
items     = spark.table(f"{SILVER}.order_items")
customers = spark.table(f"{SILVER}.customers")
sellers   = spark.table(f"{SILVER}.sellers")
products  = spark.table(f"{SILVER}.products")
payments  = spark.table(f"{SILVER}.payments")
reviews   = spark.table(f"{SILVER}.reviews")
geo       = spark.table(f"{SILVER}.geolocation").select("zip_prefix", "lat", "lng")

# COMMAND ----------
# MAGIC %md ### Dimensions

# COMMAND ----------

bounds = orders.agg(F.min(F.to_date("purchase_ts")).alias("mn"),
                    F.max(F.to_date("purchase_ts")).alias("mx")).first()
dim_date = (spark.range(1)
    .select(F.explode(F.sequence(F.lit(bounds["mn"]), F.lit(bounds["mx"]),
                                 F.expr("interval 1 day"))).alias("date"))
    .select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day"),
        F.dayofweek("date").alias("day_of_week"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend")))
write_delta(dim_date, GOLD, "dim_date")

dim_customer = (customers.join(geo, "zip_prefix", "left").select(
    "customer_id", "customer_unique_id",
    F.col("city").alias("customer_city"), F.col("state").alias("customer_state"),
    F.col("zip_prefix").alias("customer_zip_prefix"),
    F.col("lat").alias("customer_lat"), F.col("lng").alias("customer_lng")))
write_delta(dim_customer, GOLD, "dim_customer")

dim_seller = (sellers.join(geo, "zip_prefix", "left").select(
    "seller_id",
    F.col("city").alias("seller_city"), F.col("state").alias("seller_state"),
    F.col("zip_prefix").alias("seller_zip_prefix"),
    F.col("lat").alias("seller_lat"), F.col("lng").alias("seller_lng")))
write_delta(dim_seller, GOLD, "dim_seller")

dim_product = products.select("product_id", "category_pt", "category_en",
                              "weight_g", "length_cm", "height_cm", "width_cm", "photos_qty")
write_delta(dim_product, GOLD, "dim_product")

# COMMAND ----------
# MAGIC %md ### Facts

# COMMAND ----------

order_reviews = reviews.groupBy("order_id").agg(F.round(F.avg("review_score"), 2).alias("review_score"))

fact_order_items = (items.join(orders, "order_id", "inner")
    .join(order_reviews, "order_id", "left")
    .select(
        "order_id", "order_item_id", "customer_id", "product_id", "seller_id",
        F.date_format("purchase_ts", "yyyyMMdd").cast("int").alias("purchase_date_key"),
        "order_status", "price", "freight_value",
        (F.col("price") + F.col("freight_value")).alias("item_total"),
        F.datediff("delivered_ts", "purchase_ts").alias("delivery_days"),
        F.datediff("delivered_ts", "estimated_delivery_ts").alias("delivery_delay_days"),
        (F.col("delivered_ts") > F.col("estimated_delivery_ts")).alias("is_late"),
        "review_score",
        F.year("purchase_ts").alias("purchase_year"),
        F.month("purchase_ts").alias("purchase_month")))
write_delta(fact_order_items, GOLD, "fact_order_items",
            partition_by=["purchase_year", "purchase_month"])

fact_payments = (payments.join(
        orders.select("order_id", "customer_id",
                      F.date_format("purchase_ts", "yyyyMMdd").cast("int").alias("purchase_date_key")),
        "order_id", "inner")
    .select("order_id", "payment_sequential", "customer_id", "purchase_date_key",
            "payment_type", "installments", "payment_value"))
write_delta(fact_payments, GOLD, "fact_payments")

# COMMAND ----------
# MAGIC %md ### Business aggregates (delivered orders only)

# COMMAND ----------

fact = spark.table(f"{GOLD}.fact_order_items").filter(F.col("order_status") == "delivered")

monthly = (fact.groupBy("purchase_year", "purchase_month").agg(
        F.countDistinct("order_id").alias("orders"),
        F.count("*").alias("items_sold"),
        F.round(F.sum("price"), 2).alias("product_revenue"),
        F.round(F.sum("freight_value"), 2).alias("freight_revenue"),
        F.round(F.sum("price") / F.countDistinct("order_id"), 2).alias("avg_order_value"))
    .orderBy("purchase_year", "purchase_month"))
write_delta(monthly, GOLD, "agg_monthly_revenue")

category = (fact.join(spark.table(f"{GOLD}.dim_product"), "product_id")
    .groupBy("category_en").agg(
        F.count("*").alias("items_sold"),
        F.round(F.sum("price"), 2).alias("product_revenue"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score")))
write_delta(category, GOLD, "agg_category_performance")

state = (fact.join(spark.table(f"{GOLD}.dim_customer"), "customer_id")
    .groupBy("customer_state").agg(
        F.countDistinct("order_id").alias("orders"),
        F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"),
        F.round(F.avg(F.col("is_late").cast("int")) * 100, 1).alias("late_delivery_pct")))
write_delta(state, GOLD, "agg_state_delivery")

# COMMAND ----------
# MAGIC %md ### Gold validation (fails the notebook if the model is broken)

# COMMAND ----------

fact_all = spark.table(f"{GOLD}.fact_order_items")
total = fact_all.count()
distinct_keys = fact_all.select("order_id", "order_item_id").distinct().count()
null_keys = fact_all.filter(F.col("customer_id").isNull() | F.col("product_id").isNull()
                            | F.col("seller_id").isNull()).count()

assert total > 0, "fact_order_items is empty"
assert total == distinct_keys, "fact_order_items has duplicate (order_id, order_item_id)"
assert null_keys == 0, "fact_order_items has null dimension keys"
print(f"Gold validation passed: {total:,} fact rows, no duplicate or null keys.")
