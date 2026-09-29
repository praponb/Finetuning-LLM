from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("s.customer_id").isin([6, 8, 27]))
    .select(
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("we.event_time"),
        F.col("we.page"),
        F.col("we.session_id"),
    )
)
