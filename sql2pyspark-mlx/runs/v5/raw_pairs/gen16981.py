from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", "session_id")
    .pivot("event_type", ["add_to_cart", "purchase", "click"])
    .agg(F.sum("duration_sec"))
)
