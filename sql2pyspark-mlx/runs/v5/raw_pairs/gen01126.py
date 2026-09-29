from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 26) & ((F.col("event_type") == "add_to_cart") | (F.col("duration_sec") >= 240)))
    .select("event_type")
    .orderBy("event_type")
)
