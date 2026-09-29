from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .filter((F.col("we.duration_sec") > 398) | ~F.col("c.city").isin(["Singapore", "Tokyo", "New York"]))
    .select(
        F.col("we.session_id"),
        F.col("s.started_at"),
        F.col("c.email"),
        F.col("c.segment"),
    )
    .orderBy("email", "started_at")
)
