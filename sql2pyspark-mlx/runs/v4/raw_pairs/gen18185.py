from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("O"), F.col("s.id") == F.col("O.store_id"), "left")
    .filter(F.col("O.amount_count") >= 4)
    .select(F.col("s.store_name"), F.col("O.stddev_discount"), F.col("O.amount_count"))
    .orderBy("amount_count", "stddev_discount")
)
