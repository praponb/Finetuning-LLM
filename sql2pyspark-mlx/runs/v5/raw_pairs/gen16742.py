from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.hour(F.col("we.event_time")).between(9, 14))
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("s.country"),
    )
    .orderBy(F.col("id").desc(), F.col("started_at").desc())
)
