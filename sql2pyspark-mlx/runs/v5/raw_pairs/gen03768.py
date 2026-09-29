from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .filter(F.col("ss.product_id").isin([1, 4]))
    .groupBy(F.date_format(F.col("p.launch_date"), "yyyy-MM").alias("launch_month"))
    .agg(
        F.avg(F.col("ss.revenue")).alias("avg_revenue"),
        F.count("*").alias("total_count"),
    )
)
