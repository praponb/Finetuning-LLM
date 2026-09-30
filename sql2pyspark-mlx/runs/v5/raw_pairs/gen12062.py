from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 7)
    .select("event_type", "page")
    .distinct()
    .orderBy(F.col("event_type").desc())
)
