from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.started_at") >= "2023-06-25")
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("s.country"),
        F.col("s.device"),
        F.col("c.country").alias("customer_country"),
        F.col("c.credit_limit"),
    )
)
