from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.sum(F.when(F.col("channel") == "store", 1).otherwise(0)).alias("store_count"),
        F.min("order_date").alias("first_order_date"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .filter(F.col("t.store_count") > 53)
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("t.store_count"),
        F.col("t.first_order_date"),
    )
    .orderBy(F.col("store_count").desc())
)
