from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.row_count") >= 8)
    .select(F.col("s.sqft"), F.col("ss.row_count"))
    .orderBy("row_count")
)
