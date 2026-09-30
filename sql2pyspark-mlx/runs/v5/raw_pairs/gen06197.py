from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.country") == "US")
    .select(
        F.col("we.duration_sec"),
        F.col("we.session_id"),
        F.col("s.device"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.concat(F.col("s.country"), F.lit(" "), F.col("s.device")).alias("country_device"),
    )
)
