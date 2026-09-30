from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("started_at") >= "2025-06-19").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_anti")
    .filter(F.col("we.event_time") < "2025-02-01")
    .select(F.col("we.id"), F.col("we.event_type"))
)
