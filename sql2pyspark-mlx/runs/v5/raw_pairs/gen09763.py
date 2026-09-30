from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") >= "2025-06-19")
    .select("event_type", "page")
    .distinct()
)
