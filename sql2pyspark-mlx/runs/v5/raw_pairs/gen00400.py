from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([37, 39]))
    .select("event_type", "page")
    .distinct()
)
