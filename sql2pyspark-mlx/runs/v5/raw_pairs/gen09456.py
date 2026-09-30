from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.min("start_date").alias("min_start_date"),
    )
    .filter(F.col("monthly_fee_count") < 2)
)
