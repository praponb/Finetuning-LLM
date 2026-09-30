from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.store_id") == 5)
    .groupBy(F.col("c.is_active"))
    .agg(F.min(F.col("o.order_date")).alias("min_order_date"))
    .orderBy(F.col("min_order_date").desc())
)
