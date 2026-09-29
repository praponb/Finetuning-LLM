from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
    .orderBy(F.col("avg_duration_sec").desc())
)
