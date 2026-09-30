from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/search")
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(F.max("duration_sec").alias("max_duration_sec"))
    .orderBy(F.col("event_year").desc(), F.col("max_duration_sec").desc())
)
