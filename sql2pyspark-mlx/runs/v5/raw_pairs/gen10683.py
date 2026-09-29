from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_statuss"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
    .filter(F.col("avg_monthly_fee") > 40)
    .select("status", "unique_statuss", "avg_monthly_fee")
    .orderBy("unique_statuss")
)
