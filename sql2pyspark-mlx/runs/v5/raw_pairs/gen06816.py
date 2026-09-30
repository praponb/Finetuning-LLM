from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") > 301)
    .withColumn("event_time_year", F.year("event_time"))
    .withColumn("days_since_event_time", F.datediff(F.current_date(), "event_time"))
)
