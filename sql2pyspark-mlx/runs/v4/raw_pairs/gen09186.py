from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.date_trunc("WEEK", "event_time").alias("week_start"),
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
    )
    .agg(
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/search", 1)), F.lit(0)), 2).alias("duration_sec_per_search"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .filter(F.col("avg_duration_sec") > 0)
    .orderBy("quarter_start")
)
