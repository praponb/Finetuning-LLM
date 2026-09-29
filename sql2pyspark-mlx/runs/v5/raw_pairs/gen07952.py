from pyspark.sql import functions as F

kept_keys = (
    df_web_events
    .select("event_type")
    .intersect(
        df_web_events
        .filter(F.col("session_id").isin([9, 10, 35, 39]))
        .select("event_type")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("session_id", "page")
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.sum("duration_sec").alias("total_duration_sec"),
        F.max("event_time").alias("last_seen"),
    )
    .orderBy(F.col("session_id").desc_nulls_last())
)
