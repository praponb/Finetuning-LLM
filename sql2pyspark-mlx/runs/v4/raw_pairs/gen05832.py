from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") < 220)
    .orderBy(F.col("avg_duration_sec").desc())
)
