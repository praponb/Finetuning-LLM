from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("c.age") <= 31)
    .select(
        F.col("we.event_time"),
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("c.city"),
        F.col("c.country"),
        F.trim(F.col("c.city")).alias("clean_city"),
    )
    .orderBy("customer_id")
)
