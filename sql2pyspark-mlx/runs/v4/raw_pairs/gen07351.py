from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("status") == "PENDING")
    .groupBy("store_id")
    .agg(F.max("order_date").alias("latest_order_date"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("S"), F.col("s.id") == F.col("S.store_id"), "inner")
    .select(F.col("s.region"), F.col("s.store_name"), F.col("S.latest_order_date"))
)
