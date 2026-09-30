from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("amount").isNotNull())
    .groupBy("store_id")
    .agg(
        F.avg("discount").alias("avg_discount"),
        F.countDistinct("discount").alias("unique_channels"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.store_name"),
        F.col("ss.avg_discount"),
        F.col("ss.unique_channels"),
    )
    .orderBy("store_name")
)
