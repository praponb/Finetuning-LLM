from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.stddev_discount") >= 39)
    .select(F.col("s.region"), F.col("t.latest_order_date"), F.col("t.stddev_discount"))
)
