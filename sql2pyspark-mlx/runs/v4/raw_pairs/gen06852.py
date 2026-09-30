from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["click", "page_view", "purchase"]))
    .agg(
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
    .orderBy("unique_sessions")
)
