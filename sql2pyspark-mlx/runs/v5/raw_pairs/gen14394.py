from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 35) & (F.col("monthly_fee") < 140))
    .groupBy("plan")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .filter(F.col("total_monthly_fee") >= 1190)
    .orderBy("plan", F.col("total_monthly_fee").desc())
)
