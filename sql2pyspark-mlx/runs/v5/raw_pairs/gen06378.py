from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter((F.col("s.customer_id") == 32) & F.hour(F.col("s.started_at")).between(7, 11))
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("we.duration_sec"),
        F.col("s.started_at"),
    )
    .orderBy("started_at")
)
