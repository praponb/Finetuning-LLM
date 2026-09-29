from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.event_time") >= "2023-10-01")
    .select(F.col("s.browser"), F.col("we.event_time"))
)
