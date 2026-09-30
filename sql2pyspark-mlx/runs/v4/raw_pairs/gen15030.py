from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .select(F.col("we.duration_sec"), F.col("s.started_at"), F.col("s.device"))
    .orderBy(F.col("device").desc(), "started_at")
)
