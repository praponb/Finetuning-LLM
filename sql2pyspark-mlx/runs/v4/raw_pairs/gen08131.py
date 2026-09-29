from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([4, 6, 12]))
    .groupBy(
        "status",
        F.when(F.col("monthly_fee") >= 202, "high").when(F.col("monthly_fee") >= 74, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.min("start_date").alias("min_start_date"))
)
