from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .filter(F.col("s.started_at") < "2023-05-01")
    .select(
        F.col("we.duration_sec"),
        F.col("s.started_at"),
        F.when(F.col("s.device") == "mobile", 1).otherwise(0).alias("is_mobile"),
    )
)
