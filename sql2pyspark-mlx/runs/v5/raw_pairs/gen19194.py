from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 207, "high").when(F.col("monthly_fee") >= 39, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"))
    .orderBy("monthly_fee_bucket")
)
