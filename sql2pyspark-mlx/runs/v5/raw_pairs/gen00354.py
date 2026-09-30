from pyspark.sql import functions as F

kept_keys = (
    df_web_events
    .select("session_id")
    .intersectAll(
        df_web_events
        .filter(F.col("event_time") >= "2024-08-01")
        .select("session_id")
    )
)

reconciled = (
    df_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("WEEK", "event_time").alias("week_start"))
    .agg(
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_rows"),
    )
)
