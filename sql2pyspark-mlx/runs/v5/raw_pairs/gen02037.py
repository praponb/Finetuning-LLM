from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.col("session_id") == 31) & (F.col("duration_sec") <= 278)) | F.col("event_type").isin(["search", "add_to_cart", "click"]))
    .groupBy(
        F.when(F.col("event_type").isin(["purchase", "add_to_cart"]), "priority").otherwise("other").alias("event_type_group"),
    )
    .agg(F.max("event_time").alias("latest_event_time"))
    .orderBy("event_type_group", "latest_event_time")
)
