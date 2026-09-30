from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("we.session_id"),
        F.col("we.event_time"),
        F.col("s.customer_id"),
        F.col("c.signup_date"),
    )
    .orderBy("event_time", F.col("customer_id").desc())
)
