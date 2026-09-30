from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([1, 3, 8]) & (F.col("customer_id") == 24))
    .groupBy(F.quarter("start_date").alias("start_quarter"))
    .agg(
        F.max("end_date").alias("latest_end_date"),
        F.min("start_date").alias("min_start_date"),
        F.min("end_date").alias("first_end_date"),
    )
)
