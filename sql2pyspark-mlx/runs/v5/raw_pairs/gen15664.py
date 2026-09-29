from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([2, 6, 14, 39]) & (F.col("event_type") == "page_view"))
    .select("id", "event_type", "session_id")
)
