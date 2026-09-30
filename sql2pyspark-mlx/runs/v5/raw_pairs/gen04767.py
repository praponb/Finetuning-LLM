from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.email").like("%@startup.io"))
    .select(
        F.col("we.event_time"),
        F.col("we.session_id"),
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("c.city"),
        F.col("c.signup_date"),
    )
)
