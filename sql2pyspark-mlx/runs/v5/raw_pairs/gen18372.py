from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .groupBy("page")
    .pivot("event_type", ["page_view", "purchase"])
    .agg(F.sum("duration_sec"))
)
