from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.date_trunc("YEAR", "end_date").alias("year_start"),
        F.when(F.col("monthly_fee") >= 224, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_count"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee"))).alias("cancelled_total_monthly_fee"),
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
    )
)
