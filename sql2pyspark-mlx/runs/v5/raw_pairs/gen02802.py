from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNotNull() | F.col("status").isin(["EXPIRED", "CANCELLED"]))
    .groupBy(
        F.when(F.col("monthly_fee") >= 269, "high").when(F.col("monthly_fee") >= 85, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
)
