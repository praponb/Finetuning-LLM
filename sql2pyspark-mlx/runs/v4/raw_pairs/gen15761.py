from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.date_trunc("WEEK", "sale_date").alias("week_start"),
        F.when(F.col("revenue") >= 3700, "PREMIUM").otherwise("STANDARD").alias("revenue_tier"),
    )
    .agg(
        F.avg("units").alias("avg_units"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.round(F.avg("units"), 2).alias("avg_units_2"),
        F.count("*").alias("cnt"),
        F.percentile_approx("revenue", 0.95).alias("revenue_p95"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("corr_units_revenue") >= 0)
    .orderBy(F.col("avg_units").desc_nulls_first())
)
