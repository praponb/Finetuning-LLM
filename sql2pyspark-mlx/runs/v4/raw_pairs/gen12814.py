from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "left")
    .filter(F.col("obs.pct_web") >= 62)
    .select(F.col("s.id"), F.col("s.region"), F.col("obs.pct_web"))
)
