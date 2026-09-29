from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("we.session_id"),
        F.col("s.customer_id"),
        F.col("c.email"),
        F.col("c.segment"),
        F.col("s.started_at").cast("date").alias("started_at_day"),
    )
    .orderBy("email", F.col("started_at_day").desc())
)
