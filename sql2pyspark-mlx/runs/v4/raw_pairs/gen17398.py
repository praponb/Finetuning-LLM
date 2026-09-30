from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.to_date(F.col("we.event_time")) < "2023-04-01")
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.duration_sec"),
        F.col("we.page"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("s.started_at"),
    )
    .orderBy(F.col("event_type").desc())
)
