from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.month("end_date").alias("end_month"), "status")
    .agg(
        F.min("start_date").alias("min_start_date"),
        F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0)).alias("premium_count"),
    )
    .filter(F.col("premium_count") > 38)
)
