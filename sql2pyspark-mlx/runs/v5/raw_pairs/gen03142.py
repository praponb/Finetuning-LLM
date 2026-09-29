from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(10, 14))
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec") >= 489, "high").when(F.col("duration_sec") >= 72, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") < 60)
)
