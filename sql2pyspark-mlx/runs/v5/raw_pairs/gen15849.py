from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 5)
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(F.max("duration_sec").alias("max_duration_sec"))
    .filter(F.col("max_duration_sec") >= 290)
    .select("event_month", "max_duration_sec")
    .orderBy("event_month")
)
