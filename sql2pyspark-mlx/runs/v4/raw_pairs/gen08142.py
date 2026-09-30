from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter(F.col("we.session_id").isin([4, 10, 20, 37]) & F.col("we.session_id").isin([8, 35, 38]))
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("s.country"),
        F.col("s.browser"),
        F.col("c.credit_limit"),
        F.col("c.is_active"),
    )
    .orderBy("browser", "country")
)
