from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("end_date").between("2024-02-10", "2025-05-07"))
    .select("*", F.quarter("start_date").alias("start_date_quarter"))
)

result = (
    cleaned
    .groupBy(
        "customer_id",
        F.date_trunc("MONTH", "end_date").alias("activity_month"),
        F.when(F.col("monthly_fee") >= 222, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
    )
    .agg(
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
        F.percentile_approx("start_date_quarter", 0.9).alias("start_date_quarter_p90"),
        F.percentile_approx("start_date_quarter", 0.5).alias("start_date_quarter_p50"),
        F.count_if(F.col("status") == "EXPIRED").alias("expired_events"),
        F.avg(F.when(F.col("status") == "PAUSED", F.col("monthly_fee"))).alias("paused_avg_monthly_fee"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
    )
)
