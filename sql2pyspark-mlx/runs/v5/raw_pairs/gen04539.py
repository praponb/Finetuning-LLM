from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .pivot("page", ["/search", "/home", "/checkout"])
    .agg(F.max("duration_sec"))
)
