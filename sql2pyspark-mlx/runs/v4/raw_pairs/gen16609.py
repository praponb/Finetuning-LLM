from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.to_date("started_at") >= "2024-06-12").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.event_time"),
        F.col("we.page"),
    )
)
