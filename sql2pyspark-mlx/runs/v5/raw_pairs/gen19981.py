from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_trunc("YEAR", "event_time").alias("year_start"), "session_id")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.min("event_time").alias("first_event_time"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.max("event_time").alias("ended_at"),
    )
    .orderBy(F.col("duration_sec_bit_and").asc_nulls_first())
)
