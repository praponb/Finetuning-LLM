from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .filter(F.col("we.duration_sec") <= 315)
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("s.country"),
        F.col("s.device"),
        F.col("c.signup_date"),
        F.date_trunc("week", F.col("c.signup_date")).alias("signup_date_period"),
    )
    .orderBy("signup_date_period")
)
