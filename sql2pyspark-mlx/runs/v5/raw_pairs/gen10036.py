from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .groupBy("store_id")
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("obs.latest_order_date"),
        F.col("obs.shipped_count"),
    )
    .orderBy(F.col("shipped_count").desc())
)
