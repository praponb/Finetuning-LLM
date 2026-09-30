from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([5, 21, 26]))
    .groupBy("page")
    .pivot("event_type", ["page_view", "purchase", "click"])
    .agg(F.avg("duration_sec"))
    .orderBy("page")
)
