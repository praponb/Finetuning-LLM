from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter((F.col("we.duration_sec") > 157) & F.col("we.session_id").isin([4, 12]))
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("we.event_type"),
        F.col("s.started_at"),
        F.col("s.country"),
    )
    .orderBy(F.col("page").desc(), "event_type")
)
