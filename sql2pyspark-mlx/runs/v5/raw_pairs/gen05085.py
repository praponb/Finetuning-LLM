from pyspark.sql import functions as F

duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

with_dims = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "left")
    .filter(F.hour(F.col("p.started_at")).between(10, 14))
    .select(
        "e.id",
        "e.session_id",
        "e.event_type",
        "e.event_time",
        "e.duration_sec",
        F.col("p.country"),
    )
)

result = (
    with_dims
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "country",
        F.col("min_web_event_duration_sec"),
    )
    .orderBy(F.col("country").desc())
)
