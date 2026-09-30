from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_trunc("QUARTER", "event_time").alias("quarter_start"), "event_type")
    .agg(
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
    )
    .orderBy(F.col("duration_sec_p95").desc())
)
