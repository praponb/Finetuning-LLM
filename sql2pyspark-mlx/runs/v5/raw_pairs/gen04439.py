from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.city") != "Chiang Mai")
    .groupBy(F.col("c.country"))
    .agg(F.min(F.col("o.amount")).alias("min_amount"))
)
