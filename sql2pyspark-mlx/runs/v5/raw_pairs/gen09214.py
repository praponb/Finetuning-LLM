from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") != "page_view") & (F.col("session_id") == 33) & (F.col("event_type") != "page_view"))
    .select("id", "event_time", "duration_sec", "event_type")
    .orderBy(F.col("event_type").desc(), "duration_sec")
)
