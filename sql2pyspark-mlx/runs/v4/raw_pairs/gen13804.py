from pyspark.sql import functions as F

result = (
    df_web_events
    .select("id", "session_id", "event_time")
    .orderBy("event_time", F.col("session_id").desc(), "id")
    .limit(5)
)
