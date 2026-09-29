from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .pivot("event_type", ["purchase", "page_view", "search", "add_to_cart"])
    .agg(F.avg("duration_sec"))
)
