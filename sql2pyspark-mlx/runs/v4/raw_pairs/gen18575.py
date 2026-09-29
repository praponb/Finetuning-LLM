from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.status").isin(["CANCELLED", "COMPLETED", "RETURNED"]))
    .groupBy(F.col("s.city"))
    .agg(F.min(F.col("o.order_date")).alias("first_order_date"))
)
