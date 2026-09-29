from pyspark.sql import functions as F

matched_keys = (
    df_web_events
    .select("event_type", "session_id")
    .exceptAll(
        df_web_events
        .filter(F.col("page").isNotNull())
        .select("event_type", "session_id")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

result = verified.select("id", "session_id", "duration_sec")
