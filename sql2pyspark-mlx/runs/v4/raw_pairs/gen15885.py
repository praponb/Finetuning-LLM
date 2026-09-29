from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "click")
    .withColumn("duration_sec_scaled", F.round((F.col("duration_sec") * 1.15), 2))
    .orderBy("event_time")
)
