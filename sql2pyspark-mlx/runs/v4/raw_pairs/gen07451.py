from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", "session_id")
    .pivot("event_type", ["click", "purchase"])
    .agg(F.max("duration_sec").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
