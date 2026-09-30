from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .filter(F.col("sale_date") <= "2024-05-30")
    .groupBy("store_id")
    .agg(F.sum("revenue").alias("total_revenue"))
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("StoreStats"), F.col("stores.id") == F.col("StoreStats.store_id"), "inner")
    .filter(F.col("StoreStats.total_revenue") > 9000)
    .select(
        F.col("stores.opened_date"),
        F.col("stores.store_name"),
        F.col("StoreStats.total_revenue"),
    )
    .orderBy("opened_date")
)
