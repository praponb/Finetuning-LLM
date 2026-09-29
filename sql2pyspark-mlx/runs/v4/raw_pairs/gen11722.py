from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", "status")
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .filter(F.col("avg_monthly_fee") >= 240)
    .drop("avg_monthly_fee")
    .orderBy(F.col("plan").desc())
)
