from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 267, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.avg(F.when(F.col("status") == "PAUSED", F.col("monthly_fee"))).alias("paused_avg_monthly_fee"),
    )
    .orderBy(F.col("paused_avg_monthly_fee").asc_nulls_last())
)
