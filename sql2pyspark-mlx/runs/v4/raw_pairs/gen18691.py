from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(~((F.col("session_id") == 32) & (F.col("session_id") == 38) & (F.col("session_id") == 35)))
)

event_type_summary = (
    filtered_web_events
    .groupBy("event_type")
    .agg(F.min("event_time").alias("first_event_time"))
)

result = event_type_summary
