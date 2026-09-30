from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(F.count("*").alias("total_count"))
    .orderBy(F.col("total_count").desc(), "event_type")
    .limit(20)
)
