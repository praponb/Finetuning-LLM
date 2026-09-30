from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") >= "2023-11-16")
    .groupBy(
        F.quarter("end_date").alias("end_quarter"),
        F.when(F.col("monthly_fee") >= 234, "high").when(F.col("monthly_fee") >= 76, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
    .filter(F.col("stddev_monthly_fee") >= 49)
    .select("end_quarter", "monthly_fee_bucket", "stddev_monthly_fee")
    .orderBy("stddev_monthly_fee")
)
