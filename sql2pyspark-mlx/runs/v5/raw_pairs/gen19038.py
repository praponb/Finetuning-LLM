from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.customer_id") == 27)
    .groupBy(
        F.col("c.country"),
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"),
    )
    .agg(
        F.count(F.when(F.col("we.page") == "/home", 1)).alias("home_count"),
        F.max(F.col("we.event_time")).alias("last_event_time"),
    )
)
