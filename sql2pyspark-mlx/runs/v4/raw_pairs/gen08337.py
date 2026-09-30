from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([11, 26, 27]) & (F.hour("event_time").between(10, 13) | (F.col("event_type") == "add_to_cart")))
)
