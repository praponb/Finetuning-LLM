from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("event_type") != "click")
    .select(
        "*",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
    )
)

result = (
    cleaned
    .groupBy("session_id")
    .pivot("page", ["/search", "/cart"])
    .agg(F.sum("duration_sec").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "session_id",
        F.col("/search_amount").alias("search_amount"),
        F.col("/search_item_count").alias("search_item_count"),
        F.col("/cart_amount").alias("cart_amount"),
        F.col("/cart_item_count").alias("cart_item_count"),
    )
)
