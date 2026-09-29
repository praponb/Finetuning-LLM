from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") < 265) & F.col("session_id").isin([11, 17, 25, 27]) & (F.col("event_time") < "2025-04-01"))
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"), "event_type")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
)
