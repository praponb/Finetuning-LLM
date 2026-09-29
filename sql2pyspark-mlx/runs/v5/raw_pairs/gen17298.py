from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["purchase", "click"]) & (~F.col("page").isin(["/product", "/home", "/cart"]) | F.col("session_id").isin([11, 30, 38])))
    .select("event_time", "event_type", F.lower("event_type").alias("event_type_lower"))
    .orderBy("event_time", "event_type")
)
