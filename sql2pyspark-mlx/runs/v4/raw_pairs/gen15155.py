from pyspark.sql import functions as F

result = (
    df_store_sales
    .withColumn("revenue_tier", F.when(F.col("revenue") >= 4100, "HIGH").otherwise("LOW"))
    .withColumn("activity_month", F.date_trunc("MONTH", "sale_date"))
    .rollup("revenue_tier", "activity_month")
    .agg(
        F.avg("units").alias("avg_units"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.sum("units").alias("total_units"),
        F.percentile_approx("units", 0.95).alias("units_p95"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("total_units")
)
