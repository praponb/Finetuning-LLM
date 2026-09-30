from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([2, 31]))
    .groupBy("event_type")
    .agg(
        F.count("*").alias("total_count"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
    )
    .orderBy(F.col("total_count").desc(), "event_type")
)
