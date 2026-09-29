from pyspark.sql import functions as F

store_stats = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.countDistinct("shipping_fee").alias("unique_channels"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.unique_channels") >= 10)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("ss.status_list"),
        F.col("ss.unique_channels"),
    )
    .orderBy("city")
)
