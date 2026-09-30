from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .filter(F.col("monthly_fee_range") >= 62)
    .orderBy(F.col("monthly_fee_range").desc())
)
