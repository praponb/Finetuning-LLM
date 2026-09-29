from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 471, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.min("event_time").alias("first_event_time"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
    )
)
