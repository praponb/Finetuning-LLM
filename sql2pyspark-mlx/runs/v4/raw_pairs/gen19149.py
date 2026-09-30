from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([2, 13, 33]))
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
    .orderBy(F.col("max_monthly_fee").desc(), F.col("monthly_fee_count").desc())
)
