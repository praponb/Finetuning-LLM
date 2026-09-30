from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") >= "2025-05-11") & (F.col("event_type") == "page_view"))
    .orderBy("event_time")
)
