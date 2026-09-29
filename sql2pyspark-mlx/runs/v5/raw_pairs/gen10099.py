from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(F.max(F.col("o.order_date")).alias("latest_order_date"))
    .orderBy(F.col("latest_order_date").desc())
)
