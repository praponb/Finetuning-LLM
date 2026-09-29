from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("we.event_type"),
        F.col("s.country"),
        F.col("c.segment"),
        F.col("c.country").alias("customer_country"),
    )
)
