from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([18, 22, 24, 29]))
    .groupBy("event_type")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)
