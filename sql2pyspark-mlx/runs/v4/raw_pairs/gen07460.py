from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .groupBy("store_id")
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.countDistinct("discount").alias("unique_customers"),
    )
)

result = (
    df_stores.alias("stores")
    .join(orders_by_store.alias("t"), F.col("stores.id") == F.col("t.store_id"), "inner")
    .select(
        F.col("stores.id"),
        F.col("stores.city"),
        F.col("t.avg_discount"),
        F.col("t.unique_customers"),
    )
    .orderBy(F.col("unique_customers").desc())
)
