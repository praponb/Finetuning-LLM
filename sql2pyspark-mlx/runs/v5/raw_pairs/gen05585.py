from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(6, 10))
    .groupBy("page")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .filter(F.col("total_duration_sec") >= 800)
    .select("page", "total_duration_sec")
    .orderBy(F.col("total_duration_sec").desc(), "page")
)
