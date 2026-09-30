from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2024-11-01") & (F.col("duration_sec") >= 475))
    .select("event_type")
    .orderBy(F.col("event_type").desc())
)
