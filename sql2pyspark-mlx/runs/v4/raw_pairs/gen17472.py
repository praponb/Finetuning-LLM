from pyspark.sql import functions as F

remaining_keys = (
    df_web_events
    .select("page", "session_id")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec").isNotNull())
        .select("page", "session_id")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "session_id", "event_type", "page", "event_time", "duration_sec")
)
