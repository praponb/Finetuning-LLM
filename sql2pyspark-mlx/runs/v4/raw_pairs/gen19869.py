from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.page").isin(["/search", "/cart", "/home"]))
    .select(
        F.col("s.device"),
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("we.session_id"),
        F.col("we.event_type"),
        F.col("we.event_time"),
    )
)
