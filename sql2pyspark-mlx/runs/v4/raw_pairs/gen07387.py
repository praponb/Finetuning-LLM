from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([3, 18, 28]) | F.col("page").isNotNull())
    .groupBy(F.hour("event_time").alias("event_hour"), "page")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
    .orderBy(F.col("avg_duration_sec").desc())
)
