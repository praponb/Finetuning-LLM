from pyspark.sql import functions as F

duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

kept_keys = (
    df_web_events
    .select("id")
    .exceptAll(
        df_web_events
        .filter(F.col("duration_sec") < 370)
        .select("id")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
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
        F.col("min_web_event_duration_sec"),
    )
    .orderBy(F.col("session_id").desc_nulls_last(), "id")
)
