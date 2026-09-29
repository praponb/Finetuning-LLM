from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.browser") == "Chrome")
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("we.page"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("s.device"),
    )
    .orderBy("browser", "id")
    .limit(50)
)
