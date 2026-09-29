from pyspark.sql import functions as F

matched_keys = (
    df_web_events
    .select("session_id", "id")
    .subtract(
        df_web_events
        .filter(F.hour("event_time").between(7, 14))
        .select("session_id", "id")
    )
)

kept_rows = (
    df_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .rollup(
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
        "session_id",
        F.date_trunc("YEAR", "event_time").alias("year_start"),
    )
    .agg(
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.min("duration_sec").alias("min_duration_sec"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
    )
    .orderBy(F.col("year_start").desc())
)
