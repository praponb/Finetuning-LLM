from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter((F.col("s.device") == "desktop") | F.col("s.browser").isNotNull())
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("we.duration_sec"),
        F.col("we.page"),
        F.col("s.browser"),
        F.col("s.country"),
        F.col("s.started_at"),
    )
)
