from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("we.session_id"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("c.signup_date"),
    )
    .orderBy(F.col("session_id").desc())
)
