from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "inner")
    .filter(F.col("obs.total_amount") > 5900)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("obs.avg_amount"),
        F.col("obs.total_amount"),
    )
)
