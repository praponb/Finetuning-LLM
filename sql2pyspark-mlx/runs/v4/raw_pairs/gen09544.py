from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.session_id").isin([3, 10, 12, 36]))
    .select(
        F.col("we.id"),
        F.col("we.duration_sec"),
        F.col("s.browser"),
        F.col("s.device"),
        F.col("s.country"),
        F.date_format(F.col("we.event_time"), "yyyy-MM-dd").alias("event_time_label"),
    )
)
