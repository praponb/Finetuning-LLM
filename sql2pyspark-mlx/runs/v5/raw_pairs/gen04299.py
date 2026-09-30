from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("customer_id").isin([14, 32, 38])).alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .filter(F.col("we.page") == "/search")
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("we.event_type"),
        F.col("we.session_id"),
    )
)
