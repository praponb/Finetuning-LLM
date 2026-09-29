from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("s.city") == "Bangkok")
    .select(F.col("o.id"), F.col("o.customer_id"), F.col("s.store_name"))
)
