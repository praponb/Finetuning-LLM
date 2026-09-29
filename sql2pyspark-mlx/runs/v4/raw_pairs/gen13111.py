from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 418, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
        "session_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .filter(F.col("avg_duration_sec") >= 0)
)
