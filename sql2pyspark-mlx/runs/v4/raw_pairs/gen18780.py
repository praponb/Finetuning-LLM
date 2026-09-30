from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") < "2024-04-01")
    .groupBy("session_id")
    .pivot("event_type", ["click", "purchase"])
    .agg(F.sum("duration_sec"))
    .orderBy("session_id")
)
