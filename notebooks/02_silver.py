# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver layer: clean, validate, enforce integrity
# MAGIC For each table: cast types, standardise text, remove duplicate keys, apply data quality rules,
# MAGIC quarantine rejected rows in `<table>_rejected` and log every check to `dq_log`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

def ts(col):
    return F.to_timestamp(F.col(col))

# COMMAND ----------
# MAGIC %md ### Customers

# COMMAND ----------

df = spark.table(f"{BRONZE}.customers").select(
    "customer_id", "customer_unique_id",
    F.col("customer_zip_code_prefix").alias("zip_prefix"),
    F.lower(F.trim("customer_city")).alias("city"),
    F.upper(F.trim("customer_state")).alias("state"))

customers = run_quality(df, "customers", {
    "customer_id_not_null": F.col("customer_id").isNotNull(),
    "state_is_2_letters":   F.length("state") == 2,
}, key=["customer_id"])
write_delta(customers, SILVER, "customers")

# COMMAND ----------
# MAGIC %md ### Sellers

# COMMAND ----------

df = spark.table(f"{BRONZE}.sellers").select(
    "seller_id",
    F.col("seller_zip_code_prefix").alias("zip_prefix"),
    F.lower(F.trim("seller_city")).alias("city"),
    F.upper(F.trim("seller_state")).alias("state"))

sellers = run_quality(df, "sellers", {
    "seller_id_not_null": F.col("seller_id").isNotNull(),
    "state_is_2_letters": F.length("state") == 2,
}, key=["seller_id"])
write_delta(sellers, SILVER, "sellers")

# COMMAND ----------
# MAGIC %md ### Category translation and Products

# COMMAND ----------

translation = (spark.table(f"{BRONZE}.category_translation")
    .select(F.lower(F.trim("product_category_name")).alias("category_pt"),
            F.lower(F.trim("product_category_name_english")).alias("category_en"))
    .dropDuplicates(["category_pt"]))
write_delta(translation, SILVER, "category_translation")

b = (spark.table(f"{BRONZE}.products")
     # the source CSV spells these two columns "lenght"; rename is a no-op if already fixed
     .withColumnRenamed("product_name_lenght", "product_name_length")
     .withColumnRenamed("product_description_lenght", "product_description_length"))

df = (b.select(
        "product_id",
        F.coalesce(F.lower(F.trim("product_category_name")), F.lit("unknown")).alias("category_pt"),
        F.col("product_name_length").cast("int").alias("name_length"),
        F.col("product_description_length").cast("int").alias("description_length"),
        F.col("product_photos_qty").cast("int").alias("photos_qty"),
        F.col("product_weight_g").cast("double").alias("weight_g"),
        F.col("product_length_cm").cast("double").alias("length_cm"),
        F.col("product_height_cm").cast("double").alias("height_cm"),
        F.col("product_width_cm").cast("double").alias("width_cm"))
      .join(translation, "category_pt", "left")
      .withColumn("category_en", F.coalesce("category_en", "category_pt")))

products = run_quality(df, "products", {
    "product_id_not_null": F.col("product_id").isNotNull(),
    "weight_non_negative": F.col("weight_g").isNull() | (F.col("weight_g") >= 0),
}, key=["product_id"])
write_delta(products, SILVER, "products")

# COMMAND ----------
# MAGIC %md ### Geolocation (very large and duplicated -> one row per zip prefix)

# COMMAND ----------

df = spark.table(f"{BRONZE}.geolocation").select(
    F.col("geolocation_zip_code_prefix").alias("zip_prefix"),
    F.col("geolocation_lat").cast("double").alias("lat"),
    F.col("geolocation_lng").cast("double").alias("lng"),
    F.lower(F.trim("geolocation_city")).alias("city"),
    F.upper(F.trim("geolocation_state")).alias("state"))

geo_valid = run_quality(df, "geolocation", {
    "zip_prefix_not_null": F.col("zip_prefix").isNotNull(),
    "lat_inside_brazil":   F.col("lat").between(-34, 6),
    "lng_inside_brazil":   F.col("lng").between(-74, -34),
})
n_valid = geo_valid.count()
geo = geo_valid.groupBy("zip_prefix").agg(
    F.avg("lat").alias("lat"), F.avg("lng").alias("lng"),
    F.first("city").alias("city"), F.first("state").alias("state"))
log_dq("geolocation", "aggregated_to_one_row_per_zip", n_valid, n_valid - geo.count())
write_delta(geo, SILVER, "geolocation")

# COMMAND ----------
# MAGIC %md ### Orders

# COMMAND ----------

VALID_STATUSES = ["created", "approved", "invoiced", "processing", "shipped",
                  "delivered", "unavailable", "canceled"]

df = spark.table(f"{BRONZE}.orders").select(
    "order_id", "customer_id",
    F.lower(F.trim("order_status")).alias("order_status"),
    ts("order_purchase_timestamp").alias("purchase_ts"),
    ts("order_approved_at").alias("approved_ts"),
    ts("order_delivered_carrier_date").alias("carrier_ts"),
    ts("order_delivered_customer_date").alias("delivered_ts"),
    ts("order_estimated_delivery_date").alias("estimated_delivery_ts"))

orders = run_quality(df, "orders", {
    "order_id_not_null":    F.col("order_id").isNotNull(),
    "customer_id_not_null": F.col("customer_id").isNotNull(),
    "purchase_ts_valid":    F.col("purchase_ts").isNotNull(),
    "known_status":         F.col("order_status").isin(VALID_STATUSES),
    "delivery_not_before_purchase":
        F.col("delivered_ts").isNull() | (F.col("delivered_ts") >= F.col("purchase_ts")),
    "delivered_orders_have_delivery_date":
        (F.col("order_status") != "delivered") | F.col("delivered_ts").isNotNull(),
}, key=["order_id"])
orders = enforce_fk(orders, customers, "customer_id", "orders", "customer_exists")
write_delta(orders, SILVER, "orders")

# COMMAND ----------
# MAGIC %md ### Order items

# COMMAND ----------

df = spark.table(f"{BRONZE}.order_items").select(
    "order_id",
    F.col("order_item_id").cast("int").alias("order_item_id"),
    "product_id", "seller_id",
    ts("shipping_limit_date").alias("shipping_limit_ts"),
    F.col("price").cast("double").alias("price"),
    F.col("freight_value").cast("double").alias("freight_value"))

items = run_quality(df, "order_items", {
    "keys_not_null": F.col("order_id").isNotNull() & F.col("product_id").isNotNull()
                     & F.col("seller_id").isNotNull() & F.col("order_item_id").isNotNull(),
    "price_positive":        F.col("price") > 0,
    "freight_non_negative":  F.col("freight_value") >= 0,
}, key=["order_id", "order_item_id"])
items = enforce_fk(items, orders,   "order_id",   "order_items", "order_exists")
items = enforce_fk(items, products, "product_id", "order_items", "product_exists")
items = enforce_fk(items, sellers,  "seller_id",  "order_items", "seller_exists")
write_delta(items, SILVER, "order_items")

# COMMAND ----------
# MAGIC %md ### Payments

# COMMAND ----------

df = spark.table(f"{BRONZE}.payments").select(
    "order_id",
    F.col("payment_sequential").cast("int").alias("payment_sequential"),
    F.lower(F.trim("payment_type")).alias("payment_type"),
    F.col("payment_installments").cast("int").alias("installments"),
    F.col("payment_value").cast("double").alias("payment_value"))

payments = run_quality(df, "payments", {
    "order_id_not_null":    F.col("order_id").isNotNull(),
    "payment_value_non_negative": F.col("payment_value") >= 0,
    "known_payment_type":   F.col("payment_type").isin("credit_card", "boleto", "voucher", "debit_card"),
    "installments_at_least_1": F.col("installments") >= 1,
}, key=["order_id", "payment_sequential"])
payments = enforce_fk(payments, orders, "order_id", "payments", "order_exists")
write_delta(payments, SILVER, "payments")

# COMMAND ----------
# MAGIC %md ### Reviews (comment text dropped, only its length kept)

# COMMAND ----------

df = spark.table(f"{BRONZE}.reviews").select(
    "review_id", "order_id",
    F.col("review_score").cast("int").alias("review_score"),
    F.length("review_comment_message").alias("comment_length"),
    ts("review_creation_date").alias("review_created_ts"),
    ts("review_answer_timestamp").alias("review_answered_ts"))

reviews = run_quality(df, "reviews", {
    "ids_not_null":       F.col("review_id").isNotNull() & F.col("order_id").isNotNull(),
    "score_between_1_5":  F.col("review_score").between(1, 5),
}, key=["review_id", "order_id"])
reviews = enforce_fk(reviews, orders, "order_id", "reviews", "order_exists")
write_delta(reviews, SILVER, "reviews")

# COMMAND ----------

flush_dq()
display(spark.table(f"{SILVER}.dq_log").orderBy(F.desc("run_ts"), "table_name"))
