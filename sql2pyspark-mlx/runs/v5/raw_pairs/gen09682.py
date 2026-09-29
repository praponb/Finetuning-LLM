from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "purchase", "add_to_cart"]) & (F.col("event_time") >= "2024-03-30"))
    .select(
        "event_type",
        "duration_sec",
        "page",
        F.substring("event_type", 1, 4).alias("event_type_prefix"),
        F.date_trunc("month", "event_time").alias("event_time_period"),
    )
    .orderBy("duration_sec")
)
