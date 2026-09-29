from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("s.started_at"),
        F.col("c.segment"),
    )
    .orderBy(F.col("started_at").desc())
)
