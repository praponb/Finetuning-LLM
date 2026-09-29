from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "plan",
        F.when(F.col("monthly_fee") >= 247, "high").when(F.col("monthly_fee") >= 118, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
    .filter(F.col("monthly_fee_count") < 8)
    .select("plan", "monthly_fee_bucket", "monthly_fee_count", "monthly_fee_range")
)
