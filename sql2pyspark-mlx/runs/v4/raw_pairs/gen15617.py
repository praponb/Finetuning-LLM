from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "page_view")
    .withColumn("days_since_event_time", F.datediff(F.current_date(), "event_time"))
    .orderBy(F.col("event_time").desc())
)
