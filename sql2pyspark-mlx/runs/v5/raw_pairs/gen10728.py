from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["search", "add_to_cart", "page_view"]) | (F.col("event_time") < "2025-01-01"))
    .withColumn("days_since_event_time", F.datediff(F.current_date(), "event_time"))
    .withColumn("event_time_hour", F.hour("event_time"))
    .orderBy("event_time_hour", "id")
    .limit(3)
)
