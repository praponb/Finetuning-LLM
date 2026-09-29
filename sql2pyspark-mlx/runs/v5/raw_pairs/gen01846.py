from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") >= "2025-06-01") & (F.col("duration_sec") <= 271))
    .select("id", "duration_sec")
    .orderBy("id")
    .limit(50)
)
