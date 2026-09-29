from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["click", "purchase", "page_view"]))
    .select("id", "event_time", "duration_sec")
)
