from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.duration_sec") >= 265)
    .select(
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("we.page"),
        F.col("we.session_id"),
        F.col("we.event_type"),
    )
)
