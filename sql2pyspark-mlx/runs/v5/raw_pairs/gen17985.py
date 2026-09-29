from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "click")
    .groupBy("page")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
    .filter(F.col("min_duration_sec") >= 230)
    .select("page", "min_duration_sec")
)
