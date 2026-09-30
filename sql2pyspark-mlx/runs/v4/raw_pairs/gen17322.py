from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-12-31").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

result = (
    cleaned
    .withColumn("revenue_tier", F.when(F.col("revenue") >= 3500, "HIGH").otherwise("LOW"))
    .withColumn("days_since_sale_date_tier", F.when(F.col("days_since_sale_date") >= 83, "HIGH").otherwise("LOW"))
    .cube("revenue_tier", "days_since_sale_date_tier")
    .agg(
        F.percentile_approx("revenue", 0.95).alias("revenue_p95"),
        F.percentile_approx("revenue", 0.9).alias("revenue_p90"),
        F.avg("revenue").alias("avg_revenue"),
        F.min("days_since_sale_date").alias("min_days_since_sale_date"),
        F.sum("revenue").alias("total_revenue"),
        F.grouping_id().alias("gid"),
    )
)
