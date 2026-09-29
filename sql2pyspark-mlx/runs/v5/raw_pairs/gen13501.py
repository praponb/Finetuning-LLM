from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") <= 451) | F.col("session_id").isin([1, 16, 18]))
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"), "session_id")
    .agg(F.min("event_time").alias("min_event_time"))
    .orderBy(F.col("session_id").desc(), F.col("min_event_time").desc())
)
