from pyspark.sql import functions as F

duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.sum("duration_sec").alias("sum_web_event_duration_sec"))

remaining_keys = (
    df_web_events
    .select("id", "session_id")
    .exceptAll(
        df_web_events
        .filter(F.col("page") != "/search")
        .select("id", "session_id")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
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
        F.col("sum_web_event_duration_sec"),
    )
    .orderBy(F.col("event_type").desc_nulls_first())
)
