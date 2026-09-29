from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 3400, "LARGE").otherwise("SMALL").alias("revenue_tier"),
        "store_id",
    )
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.percentile_approx("units", 0.95).alias("units_p95"),
        F.count("*").alias("event_count"),
        F.sum("revenue").alias("total_revenue"),
    )
    .orderBy(F.col("total_revenue").desc_nulls_first())
)
