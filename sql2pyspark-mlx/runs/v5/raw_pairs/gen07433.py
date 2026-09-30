from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .filter((F.col("s.customer_id") == 16) & (F.col("s.started_at") >= "2023-07-01"))
    .select(
        F.col("we.event_time"),
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("c.age"),
        F.col("c.segment"),
    )
)
