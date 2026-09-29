from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(152, 210))
    .groupBy("session_id")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
    .orderBy("avg_duration_sec")
)
