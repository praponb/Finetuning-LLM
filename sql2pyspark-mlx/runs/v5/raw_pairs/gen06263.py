from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.when(F.col("units") >= 35, "HIGH").otherwise("LOW").alias("units_tier"))
    .agg(
        F.bit_or("units").alias("units_bit_or"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.avg("revenue").alias("avg_revenue"),
        F.avg("units").alias("avg_units"),
        F.percentile_approx("units", 0.95).alias("units_p95"),
    )
    .filter(F.col("corr_units_revenue") > 0)
)
