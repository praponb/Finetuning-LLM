from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
    .filter(F.col("stddev_duration_sec") > 52)
)
