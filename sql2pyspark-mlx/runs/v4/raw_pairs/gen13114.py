from pyspark.sql import functions as F

result = (
    df_web_events
    .cube(F.date_trunc("MONTH", "event_time").alias("activity_month"), "page")
    .agg(
        F.count("*").alias("row_count"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
    )
    .filter(F.col("row_count") >= 1)
)
