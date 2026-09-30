from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .pivot("event_type", ["purchase", "click", "add_to_cart"])
    .agg(F.avg("duration_sec"))
    .orderBy("page")
)
