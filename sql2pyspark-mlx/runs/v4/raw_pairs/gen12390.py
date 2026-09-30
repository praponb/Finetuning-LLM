from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", "status")
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .filter(F.col("min_monthly_fee") >= 190)
    .orderBy(F.col("plan").desc(), "status")
)
