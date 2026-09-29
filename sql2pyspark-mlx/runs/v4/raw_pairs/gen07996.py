from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("we.duration_sec") != 357)
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("c.is_active"),
        F.to_date(F.col("s.started_at")).alias("started_at_date"),
    )
)
