from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") >= 352)
    .select("id", "page", "event_type")
    .orderBy(F.col("page").desc(), F.col("event_type").desc())
)
