from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.city"))
    .agg(
        F.max(F.col("we.duration_sec")).alias("max_duration_sec"),
        F.min(F.col("we.event_time")).alias("min_event_time"),
    )
    .orderBy(F.col("max_duration_sec").desc())
)
