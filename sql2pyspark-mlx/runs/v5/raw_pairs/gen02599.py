from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("status") == "PAUSED", 1)), F.lit(0)), 2).alias("monthly_fee_per_paused"),
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee")).otherwise(0)).alias("expired_monthly_fee"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("expired_monthly_fee").desc())
)
