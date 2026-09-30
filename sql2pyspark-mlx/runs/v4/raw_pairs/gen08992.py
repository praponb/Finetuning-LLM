from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(~F.col("s.country").isin(["UK", "SG"]))
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.duration_sec"),
        F.col("we.page"),
        F.col("s.country"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.date_trunc("week", F.col("we.event_time")).alias("event_time_period"),
    )
)
