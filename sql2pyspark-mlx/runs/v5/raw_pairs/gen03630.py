from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("we.session_id").isin([21, 26, 33, 37]))
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("we.event_type"),
        F.col("s.customer_id"),
        F.col("c.city"),
        F.col("c.email"),
    )
    .orderBy("email")
)
