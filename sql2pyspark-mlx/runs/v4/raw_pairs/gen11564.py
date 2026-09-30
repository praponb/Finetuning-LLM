from pyspark.sql import functions as F

kept_keys = (
    df_web_events
    .select("session_id", "page")
    .subtract(
        df_web_events
        .filter(F.col("event_type") == "add_to_cart")
        .select("session_id", "page")
    )
)

kept_rows = (
    df_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("QUARTER", "event_time").alias("quarter_start"))
    .agg(
        F.count("*").alias("cnt"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
        F.min("event_time").alias("first_event_time"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .orderBy(F.col("duration_sec_bit_and").desc_nulls_first())
)
