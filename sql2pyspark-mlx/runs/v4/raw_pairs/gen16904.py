from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(F.col("revenue").isNotNull() & (F.col("sale_date") < "2025-01-24"))
    .withColumn("units_filled", F.coalesce("units", F.lit(0)))
)

store_sales_stats = (
    filtered_store_sales
    .groupBy(
        F.date_format("sale_date", "yyyy-MM").alias("sale_month"),
        F.year("sale_date").alias("sale_year"),
    )
    .agg(F.count("revenue").alias("revenue_count"))
)

result = (
    store_sales_stats
    .select("sale_month", "revenue_count")
    .orderBy(F.col("revenue_count").desc(), "sale_month")
)
