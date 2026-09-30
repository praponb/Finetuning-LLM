from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .pivot("page", ["/product", "/checkout", "/cart"])
    .agg(F.sum("duration_sec"))
)
