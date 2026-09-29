from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .select(F.col("s.device"), F.col("we.event_type"), F.col("we.session_id"))
)
