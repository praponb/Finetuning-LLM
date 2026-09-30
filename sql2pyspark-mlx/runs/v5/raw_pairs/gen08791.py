from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .pivot("event_type", ["purchase", "page_view"])
    .agg(F.avg("duration_sec"))
    .orderBy("page")
)
