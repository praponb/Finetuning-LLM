from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.device").isin(["desktop", "tablet", "mobile"]))
    .select(
        F.col("we.event_time"),
        F.col("we.page"),
        F.col("we.event_type"),
        F.col("s.customer_id"),
        F.col("s.browser"),
    )
    .orderBy("event_type")
)
