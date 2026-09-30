from pyspark.sql import functions as F

remaining_keys = (
    df_web_events
    .select("session_id")
    .intersect(
        df_web_events
        .filter(F.col("page").isNull())
        .select("session_id")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("WEEK", "event_time").alias("week_start"))
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
    .orderBy(F.col("duration_sec_stddev").asc_nulls_last())
)
