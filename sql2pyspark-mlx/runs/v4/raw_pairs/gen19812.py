from pyspark.sql import functions as F

remaining_keys = (
    df_web_events
    .select("event_type", "session_id")
    .intersectAll(
        df_web_events
        .filter(F.col("session_id").isin([4, 18]))
        .select("event_type", "session_id")
    )
)

reconciled = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("session_id", "event_type", "page", "event_time", "duration_sec")
    .orderBy(F.col("event_time").desc())
)
