from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "customer_id",
        F.when(F.col("monthly_fee") >= 224, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_tier"),
        F.date_trunc("WEEK", "end_date").alias("week_start"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee"))).alias("enterprise_total_monthly_fee"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "premium", 1)), F.lit(0)), 2).alias("monthly_fee_per_premium"),
    )
    .orderBy("monthly_fee_tier")
)
