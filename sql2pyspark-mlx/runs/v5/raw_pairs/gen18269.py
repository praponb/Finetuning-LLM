from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .groupBy("page")
    .pivot("event_type", ["add_to_cart", "page_view", "purchase", "search"])
    .agg(F.sum("duration_sec"))
    .orderBy("page")
)
