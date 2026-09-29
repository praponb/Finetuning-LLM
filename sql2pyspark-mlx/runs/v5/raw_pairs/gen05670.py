from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .groupBy(F.col("s.device"))
    .agg(
        F.min(F.col("we.event_time")).alias("first_event_time"),
        F.max(F.col("we.event_time")).alias("last_event_time"),
    )
)
