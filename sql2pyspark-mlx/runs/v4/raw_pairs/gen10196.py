from pyspark.sql import functions as F

duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

with_dims = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("b"), F.col("a.session_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.browser"), F.col("b.country"), F.col("b.device"))
)

result = (
    with_dims
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.col("max_web_event_duration_sec"),
    )
    .orderBy(F.col("duration_sec").desc(), F.col("id").desc())
)
