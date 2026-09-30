from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", "session_id")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
    )
)
