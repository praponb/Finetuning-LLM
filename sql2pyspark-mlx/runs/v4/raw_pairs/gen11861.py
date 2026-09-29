from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(F.avg("discount").alias("avg_discount"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.avg_discount") >= 0.19)
    .select(F.col("s.id"), F.col("s.city"), F.col("s.region"), F.col("ss.avg_discount"))
)
