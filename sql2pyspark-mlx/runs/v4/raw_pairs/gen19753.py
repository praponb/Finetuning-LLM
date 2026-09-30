from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "outer")
    .filter(F.col("we.duration_sec") == 232)
    .select(
        F.col("we.duration_sec"),
        F.col("we.event_time"),
        F.col("s.country"),
        F.col("s.browser"),
        F.col("s.customer_id"),
    )
    .orderBy("event_time")
)
