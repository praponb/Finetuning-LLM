from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 1) & (F.col("event_type") != "click"))
    .select("id", "event_type")
)
