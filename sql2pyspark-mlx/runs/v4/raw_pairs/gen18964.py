from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("revenue") >= 1800)
    .withColumn("revenue_per_unit", F.round((F.col("units") / F.col("revenue")), 2))
)

result = (
    filtered
    .groupBy(
        F.date_trunc("YEAR", "sale_date").alias("year_start"),
        F.when(F.col("units") >= 44, "PREMIUM").otherwise("STANDARD").alias("units_tier"),
    )
    .agg(
        F.percentile_approx("revenue", 0.95).alias("revenue_p95"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
