from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .pivot("event_type", ["search", "click", "page_view"])
    .agg(F.sum("duration_sec"))
    .orderBy("page")
)
