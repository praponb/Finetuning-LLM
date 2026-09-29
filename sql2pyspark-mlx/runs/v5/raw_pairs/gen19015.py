from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("duration_sec_scaled", F.round((F.col("duration_sec") * 100), 2))
    .withColumn("event_time_label", F.date_format("event_time", "yyyy-MM"))
    .orderBy("duration_sec_scaled", "id")
    .limit(20)
)
