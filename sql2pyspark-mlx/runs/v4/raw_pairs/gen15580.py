from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "right")
    .filter(F.col("s.device") == "tablet")
    .select(
        F.col("we.session_id"),
        F.col("we.duration_sec"),
        F.col("s.country"),
        F.col("s.customer_id"),
        F.initcap(F.trim(F.col("we.event_type"))).alias("event_type_title"),
    )
)
