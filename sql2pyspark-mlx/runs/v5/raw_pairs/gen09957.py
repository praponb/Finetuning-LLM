from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", "session_id")
    .pivot("page", ["/checkout", "/search"])
    .agg(F.avg("duration_sec").alias("total"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "event_type",
        "session_id",
        F.col("/checkout_total").alias("checkout_total"),
        F.col("/checkout_item_count").alias("checkout_item_count"),
        F.col("/search_total").alias("search_total"),
        F.col("/search_item_count").alias("search_item_count"),
    )
)
