from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 186, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.percentile_approx("monthly_fee", 0.95).alias("monthly_fee_p95"),
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.percentile_approx("monthly_fee", 0.9).alias("monthly_fee_p90"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee"))).alias("premium_total_monthly_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(F.col("monthly_fee_p90").desc_nulls_first())
)
