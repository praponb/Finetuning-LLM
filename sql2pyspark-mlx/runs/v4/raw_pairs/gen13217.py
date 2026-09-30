from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") >= "2025-01-01")
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec") >= 468, "high").when(F.col("duration_sec") >= 193, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.count("*").alias("row_count"),
    )
    .orderBy("row_count", "event_type")
)
