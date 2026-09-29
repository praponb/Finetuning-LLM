from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .filter(F.col("sale_date") >= "2025-07-01")
    .groupBy("store_id")
    .agg(F.max("revenue").alias("max_revenue"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.max_revenue") > 1600)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("t.max_revenue"),
    )
    .orderBy("city")
)
