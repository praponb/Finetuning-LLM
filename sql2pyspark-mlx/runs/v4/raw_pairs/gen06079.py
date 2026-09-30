from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 209, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
    )
    .orderBy(F.col("avg_monthly_fee").asc_nulls_last())
)
