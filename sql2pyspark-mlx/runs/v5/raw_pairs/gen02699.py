from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .pivot("page", ["/search", "/product"])
    .agg(F.sum("duration_sec"))
    .orderBy("event_type")
)
