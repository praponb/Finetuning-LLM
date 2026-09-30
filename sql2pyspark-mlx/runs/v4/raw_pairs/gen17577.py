from pyspark.sql import functions as F

joined = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("b"), F.col("a.session_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.country"))
)

result = (
    joined
    .rollup(
        F.when(F.col("duration_sec") >= 512, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
    )
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
    )
    .filter(F.col("avg_duration_sec") < 0)
    .orderBy(F.col("activity_month").asc_nulls_first())
)
