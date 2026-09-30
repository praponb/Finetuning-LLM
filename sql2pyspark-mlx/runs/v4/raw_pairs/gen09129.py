from pyspark.sql import functions as F

remaining_keys = (
    df_web_events
    .select("session_id", "event_type")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec") <= 120)
        .select("session_id", "event_type")
    )
)

kept_rows = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

result = kept_rows.select("id", "duration_sec")
