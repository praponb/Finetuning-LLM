from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("device") == "desktop").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_anti")
    .filter(F.to_date(F.col("we.event_time")) < "2024-02-01")
    .select(F.col("we.id"), F.col("we.duration_sec"), F.col("we.event_time"))
)
