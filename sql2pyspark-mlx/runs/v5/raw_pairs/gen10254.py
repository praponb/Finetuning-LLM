from pyspark.sql import functions as F

recent_web_events = df_web_events.filter(F.col("duration_sec") >= 277)

summary = (
    recent_web_events
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
)

result = (
    summary
    .filter(F.col("stddev_duration_sec") > 55)
    .orderBy(F.col("stddev_duration_sec").desc(), "event_month")
)
