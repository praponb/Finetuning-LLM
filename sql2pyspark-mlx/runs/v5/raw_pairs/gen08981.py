from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter((F.to_date(F.col("s.started_at")) >= "2024-02-27") & F.hour(F.col("s.started_at")).between(12, 19))
    .select(
        F.col("we.id"),
        F.col("we.duration_sec"),
        F.col("we.page"),
        F.col("s.customer_id"),
        F.col("c.age"),
        F.col("c.email"),
    )
)
