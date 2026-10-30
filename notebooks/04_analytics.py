# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Analytics on the gold layer (Spark SQL with window functions)

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------
# MAGIC %md ### 1. Top 3 sellers by revenue in each state (RANK)

# COMMAND ----------

display(spark.sql(f"""
WITH seller_revenue AS (
    SELECT s.seller_state, f.seller_id, ROUND(SUM(f.price), 2) AS revenue
    FROM {GOLD}.fact_order_items f
    JOIN {GOLD}.dim_seller s ON f.seller_id = s.seller_id
    WHERE f.order_status = 'delivered'
    GROUP BY s.seller_state, f.seller_id
)
SELECT * FROM (
    SELECT *, RANK() OVER (PARTITION BY seller_state ORDER BY revenue DESC) AS rnk
    FROM seller_revenue
) WHERE rnk <= 3
ORDER BY seller_state, rnk
"""))

# COMMAND ----------
# MAGIC %md ### 2. Month-over-month revenue growth (LAG)
# MAGIC 2016 has only 3 months of sparse data (and no November), so growth is reported from 2017 onward.

# COMMAND ----------

display(spark.sql(f"""
SELECT purchase_year, purchase_month, product_revenue,
       LAG(product_revenue) OVER (ORDER BY purchase_year, purchase_month) AS previous_month,
       ROUND(100 * (product_revenue - LAG(product_revenue) OVER (ORDER BY purchase_year, purchase_month))
             / LAG(product_revenue) OVER (ORDER BY purchase_year, purchase_month), 1) AS mom_growth_pct
FROM {GOLD}.agg_monthly_revenue
WHERE purchase_year >= 2017
ORDER BY purchase_year, purchase_month
"""))

# COMMAND ----------
# MAGIC %md ### 3. Running (cumulative) revenue

# COMMAND ----------

display(spark.sql(f"""
SELECT purchase_year, purchase_month, product_revenue,
       ROUND(SUM(product_revenue) OVER (ORDER BY purchase_year, purchase_month
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS cumulative_revenue
FROM {GOLD}.agg_monthly_revenue
ORDER BY purchase_year, purchase_month
"""))

# COMMAND ----------
# MAGIC %md ### 4. Category share of total revenue (SUM OVER)

# COMMAND ----------

display(spark.sql(f"""
SELECT category_en, product_revenue,
       ROUND(100 * product_revenue / SUM(product_revenue) OVER (), 2) AS revenue_share_pct,
       RANK() OVER (ORDER BY product_revenue DESC) AS revenue_rank
FROM {GOLD}.agg_category_performance
ORDER BY revenue_rank
LIMIT 15
"""))

# COMMAND ----------
# MAGIC %md ### 5. Repeat-customer rate (uses customer_unique_id)

# COMMAND ----------

display(spark.sql(f"""
WITH per_customer AS (
    SELECT c.customer_unique_id, COUNT(DISTINCT f.order_id) AS n_orders
    FROM {GOLD}.fact_order_items f
    JOIN {GOLD}.dim_customer c ON f.customer_id = c.customer_id
    WHERE f.order_status = 'delivered'
    GROUP BY c.customer_unique_id
)
SELECT COUNT(*) AS customers,
       SUM(CASE WHEN n_orders > 1 THEN 1 ELSE 0 END) AS repeat_customers,
       ROUND(100 * SUM(CASE WHEN n_orders > 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS repeat_rate_pct
FROM per_customer
"""))
