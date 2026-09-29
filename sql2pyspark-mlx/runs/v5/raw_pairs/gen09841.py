from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull() & F.col("event_type").isin(["page_view", "search", "click"]))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.hour("event_time").alias("event_time_hour"),
        F.concat_ws("/", "event_type", "page").alias("event_type_page_key"),
    )
)

result = (
    filtered
    .groupBy("session_id")
    .agg(
        F.min("event_time").alias("first_seen"),
        F.count_if(F.col("page") == "/cart").alias("cart_events"),
    )
)
