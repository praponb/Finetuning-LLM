from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .select(F.col("o.id"), F.col("o.channel"), F.col("s.store_name"), F.col("s.region"))
)
