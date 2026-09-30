from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .filter(F.col("units") < 16)
    .groupBy("store_id")
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.countDistinct("units").alias("unique_products"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.unique_products") >= 10)
    .select(F.col("s.store_name"), F.col("ss.avg_revenue"), F.col("ss.unique_products"))
)
