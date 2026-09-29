from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
    .orderBy(F.col("duration_sec_range").desc())
)
