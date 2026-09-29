from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 180) & F.col("end_date").between("2024-10-10", "2025-10-01"))
    .groupBy(
        "status",
        F.when(F.col("monthly_fee") >= 190, "high").when(F.col("monthly_fee") >= 99, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
    .orderBy("avg_monthly_fee")
)
