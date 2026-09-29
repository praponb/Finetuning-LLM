from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([18, 30]))
    .groupBy("event_type")
    .agg(
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .filter(F.col("purchase_count") < 5)
)
