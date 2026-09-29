from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("we.event_type") == "search")
    .select(
        F.col("we.session_id"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("c.city"),
        F.col("c.age"),
        F.regexp_extract(F.col("c.email"), "@(.+)$", 1).alias("email_domain"),
    )
)
