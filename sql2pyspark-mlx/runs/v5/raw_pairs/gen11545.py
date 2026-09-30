from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(191, 416))
    .select("id", "duration_sec", "event_type", "session_id")
    .orderBy(F.col("event_type").desc(), "duration_sec", "id")
    .limit(50)
)
