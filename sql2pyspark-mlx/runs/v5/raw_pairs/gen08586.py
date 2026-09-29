from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([4, 8, 12]) & (F.col("event_type") == "page_view") & (F.to_date("event_time") < "2024-09-16"))
    .groupBy("page", F.quarter("event_time").alias("event_quarter"))
    .agg(F.max("duration_sec").alias("max_duration_sec"))
    .filter(F.col("max_duration_sec") < 390)
    .orderBy("event_quarter")
)
