from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", "status")
    .agg(
        F.min("start_date").alias("min_start_date"),
        F.max("end_date").alias("latest_end_date"),
    )
    .orderBy("plan")
)
