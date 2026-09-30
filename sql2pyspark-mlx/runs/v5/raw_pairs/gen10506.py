from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([1, 2, 26]))
    .groupBy("event_type")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .filter(F.col("total_duration_sec") >= 1300)
    .select("event_type", "total_duration_sec")
    .orderBy("total_duration_sec")
)
