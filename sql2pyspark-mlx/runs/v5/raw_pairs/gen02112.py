from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("c.country"))
    .agg(F.max(F.col("o.order_date")).alias("latest_order_date"))
    .orderBy("country")
    .limit(1)
)
