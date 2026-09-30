from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id", "page")
    .pivot("event_type", ["page_view", "click"])
    .agg(F.avg("duration_sec").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
