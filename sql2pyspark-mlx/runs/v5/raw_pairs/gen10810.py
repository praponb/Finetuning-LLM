from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.to_date("event_time").alias("event_day"))
    .pivot("page", ["/checkout", "/search"])
    .agg(F.sum("duration_sec"))
)
