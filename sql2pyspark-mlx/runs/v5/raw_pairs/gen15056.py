from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(166, 283))
    .withColumn("days_since_event_time", F.datediff(F.current_date(), "event_time"))
    .withColumn("event_time_quarter", F.quarter("event_time"))
    .orderBy("event_time")
)
