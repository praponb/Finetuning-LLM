from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.sum("revenue").alias("total_revenue"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("t.revenue_range"),
        F.col("t.total_revenue"),
    )
)
