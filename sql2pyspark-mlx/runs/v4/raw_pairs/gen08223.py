from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .filter(F.col("revenue") >= 2000)
    .groupBy("store_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.min("revenue").alias("min_revenue"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("ss.total_revenue"),
        F.col("ss.min_revenue"),
    )
    .orderBy(F.col("total_revenue").desc())
)
