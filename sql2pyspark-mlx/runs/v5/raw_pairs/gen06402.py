from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.country").isin(["US", "SG"]))
    .select(
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("we.event_type"),
        F.col("we.duration_sec"),
        F.col("we.event_time"),
    )
)
