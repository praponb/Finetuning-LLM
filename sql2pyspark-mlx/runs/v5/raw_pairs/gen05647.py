from pyspark.sql import functions as F

result = (
    df_subscriptions
    .cube(
        F.when(F.col("monthly_fee") >= 231, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_rows"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("monthly_fee_per_enterprise"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("monthly_fee")).otherwise(0)).alias("active_monthly_fee"),
        F.sum(F.when(F.col("plan") == "standard", F.col("monthly_fee")).otherwise(0)).alias("standard_monthly_fee"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
    .orderBy(F.col("active_monthly_fee").desc_nulls_last())
)
