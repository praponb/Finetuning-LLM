from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("page").isNull())
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0)).alias("cart_count"),
        F.min("event_time").alias("first_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("t.cart_count"),
        F.col("t.first_event_time"),
    )
)
