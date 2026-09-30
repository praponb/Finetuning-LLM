from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 227, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_tier"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee"))).alias("expired_total_monthly_fee"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
    )
    .filter(F.col("expired_total_monthly_fee") > 0)
)
