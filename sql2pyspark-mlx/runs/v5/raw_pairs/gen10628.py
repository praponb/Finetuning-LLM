from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "outer")
    .filter((F.col("we.duration_sec") >= 149) & (F.col("s.country") != "UK"))
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("we.duration_sec"),
        F.col("we.event_time"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("s.country"),
    )
)
