from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") < 135)
    .groupBy("event_type")
    .pivot("page", ["/cart", "/home", "/checkout", "/search"])
    .agg(F.sum("duration_sec"))
    .orderBy("event_type")
)
