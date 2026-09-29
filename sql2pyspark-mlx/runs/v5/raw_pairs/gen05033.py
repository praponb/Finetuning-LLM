from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(12, 19) & (F.col("event_time") < "2025-05-01"))
    .cube("event_type", "page")
    .agg(F.countDistinct("duration_sec").alias("unique_pages"))
    .orderBy(F.col("unique_pages").desc())
)
