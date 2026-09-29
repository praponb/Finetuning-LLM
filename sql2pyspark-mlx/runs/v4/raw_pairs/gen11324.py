from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") <= 305) & (F.col("duration_sec") >= 181))
    .groupBy("event_type")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .filter(F.col("total_duration_sec") > 2600)
    .drop("total_duration_sec")
    .orderBy("duration_sec_count")
)
