from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter((F.col("we.duration_sec") > 138) | (F.col("s.browser") == "Edge"))
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.page"),
        F.col("s.customer_id"),
        F.col("c.signup_date"),
        F.col("c.is_active"),
    )
    .orderBy(F.col("id").desc())
)
