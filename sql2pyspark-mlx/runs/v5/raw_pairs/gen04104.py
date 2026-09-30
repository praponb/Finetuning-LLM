from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("channel") == "web")
    .groupBy("store_id")
    .agg(
        F.max("discount").alias("max_discount"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("O"), F.col("s.id") == F.col("O.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("O.max_discount"),
        F.col("O.avg_discount"),
    )
)
