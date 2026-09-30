from pyspark.sql import functions as F

remaining_keys = (
    df_web_events
    .select("event_type", "id")
    .intersectAll(
        df_web_events
        .filter(F.hour("event_time").between(8, 11))
        .select("event_type", "id")
    )
)

reconciled = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "session_id", "event_type", "page", "event_time", "duration_sec")
    .orderBy(F.col("session_id").asc_nulls_first())
)
