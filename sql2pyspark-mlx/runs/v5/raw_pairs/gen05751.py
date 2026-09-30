from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 268, "high").when(F.col("monthly_fee") >= 79, "mid").otherwise("low").alias("monthly_fee_bucket"),
        "plan",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .filter(F.col("min_monthly_fee") < 210)
    .drop("min_monthly_fee")
)
