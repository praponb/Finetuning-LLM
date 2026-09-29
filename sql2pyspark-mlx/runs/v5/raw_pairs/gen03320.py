from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 231, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_count"),
        F.percentile_approx("monthly_fee", 0.5).alias("monthly_fee_p50"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
    .orderBy(F.col("monthly_fee_p50").desc_nulls_first())
)
