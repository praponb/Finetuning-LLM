from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.browser") == "Chrome")
    .groupBy(
        F.col("c.city"),
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"),
    )
    .agg(
        F.sum(F.when(F.col("we.event_type") == "add_to_cart", F.col("we.duration_sec")).otherwise(0)).alias("add_to_cart_duration_sec"),
        F.min(F.col("we.duration_sec")).alias("min_duration_sec"),
    )
    .orderBy("add_to_cart_duration_sec", F.col("min_duration_sec").desc())
)
