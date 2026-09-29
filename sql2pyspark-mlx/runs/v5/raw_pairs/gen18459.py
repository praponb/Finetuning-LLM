from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.count("revenue").alias("revenue_count"),
    )
    .filter(F.col("revenue_count") >= 4)
    .select("sale_month", "avg_revenue", "revenue_count")
)
