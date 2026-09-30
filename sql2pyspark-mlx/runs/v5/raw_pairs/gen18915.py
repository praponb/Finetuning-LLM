from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .rollup("event_type", "session_id")
    .agg(F.max("event_time").alias("last_event_time"))
    .orderBy("event_type")
)
