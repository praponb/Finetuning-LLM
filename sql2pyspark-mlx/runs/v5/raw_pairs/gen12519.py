from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id", "page")
    .pivot("event_type", ["add_to_cart", "click"])
    .agg(F.max("duration_sec").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
