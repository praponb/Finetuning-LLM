from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(F.count("duration_sec").alias("duration_sec_count"))
    .filter(F.col("duration_sec_count") < 5)
    .orderBy("duration_sec_count")
)
