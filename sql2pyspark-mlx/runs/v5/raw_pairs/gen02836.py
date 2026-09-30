from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .filter(F.col("product_id").isin([5, 10]))
    .groupBy("store_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.avg("revenue").alias("avg_revenue"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .filter(F.col("t.avg_revenue") >= 1700)
    .select(F.col("s.city"), F.col("t.total_units"), F.col("t.avg_revenue"))
)
