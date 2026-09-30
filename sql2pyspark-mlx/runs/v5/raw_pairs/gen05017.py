from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("we.event_type"),
        F.col("we.session_id"),
        F.col("s.customer_id"),
    )
    .orderBy(F.col("session_id").desc(), F.col("customer_id").desc())
)
