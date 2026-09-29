from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["add_to_cart", "search"]))
    .select("event_time", "event_type", "session_id")
    .orderBy(F.col("event_type").desc())
)
