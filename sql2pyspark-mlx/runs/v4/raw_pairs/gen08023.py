from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(F.avg("shipping_fee").alias("avg_shipping_fee"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("obs.avg_shipping_fee"),
    )
)
