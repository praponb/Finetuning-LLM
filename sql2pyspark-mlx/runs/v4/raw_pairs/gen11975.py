from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
    .orderBy(F.col("total_duration_sec").desc(), "session_id")
    .limit(10)
)
