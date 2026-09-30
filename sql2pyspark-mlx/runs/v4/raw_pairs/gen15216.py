from pyspark.sql import functions as F

result = (
    df_web_events
    .cube(
        F.when(F.col("duration_sec") >= 536, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles_2"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("span_minutes") > 0)
    .orderBy(F.col("duration_sec_tier").desc_nulls_last())
)
