from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .filter(F.col("sale_date") < "2023-11-07")
    .groupBy("store_id")
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("ss.min_sale_date"),
        F.col("ss.total_count"),
    )
)
