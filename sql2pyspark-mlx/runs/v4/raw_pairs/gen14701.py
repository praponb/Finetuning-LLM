from pyspark.sql import functions as F

duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

kept_keys = (
    df_web_events
    .select("session_id")
    .intersect(
        df_web_events
        .filter(F.col("event_type") == "click")
        .select("session_id")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

result = (
    verified
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
    .orderBy(F.col("event_time").desc_nulls_first())
)
