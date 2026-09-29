from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .filter(F.col("total_duration_sec") < 900)
    .orderBy(F.col("event_year").desc())
)
