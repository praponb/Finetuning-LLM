from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.duration_sec") >= 185)
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.date_trunc("YEAR", F.col("we.event_time")).alias("event_time_period"),
    )
)
