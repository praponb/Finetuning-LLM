from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 99, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.count_if(F.col("status") == "DELIVERED").alias("delivered_events"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_rows"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.count_if(F.col("carrier") == "UPS").alias("ups_events"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
    )
    .orderBy(F.col("delivered_rows").asc_nulls_last())
)
