from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.hour(F.col("s.started_at")).between(9, 16))
    .select(
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("we.page"),
        F.col("we.session_id"),
        F.col("we.duration_sec"),
    )
)
