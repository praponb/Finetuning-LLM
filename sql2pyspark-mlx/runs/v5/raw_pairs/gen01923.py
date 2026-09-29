from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") >= "2024-03-01") & F.col("session_id").isin([3, 11]))
    .select("duration_sec", "page", F.lower("event_type").alias("event_type_lower"))
)
