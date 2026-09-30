from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_store_sales.alias("store_sales"), F.col("stores.id") == F.col("store_sales.store_id"), "left")
    .filter(F.col("stores.city") == "Chiang Mai")
    .groupBy(F.col("stores.id"), F.col("stores.region"))
    .agg(
        F.count(F.col("store_sales.id")).alias("store_sale_count"),
        F.sum(F.col("store_sales.revenue")).alias("total_revenue"),
    )
)
