from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") > 110) | F.col("customer_id").isin([2, 3, 24, 35]))
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
)
