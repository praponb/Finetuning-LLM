from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", "session_id")
    .pivot("page", ["/cart", "/product", "/search"])
    .agg(F.sum("duration_sec").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "event_type",
        "session_id",
        F.col("/cart_value_sum").alias("cart_value_sum"),
        F.col("/cart_cnt").alias("cart_cnt"),
        F.col("/product_value_sum").alias("product_value_sum"),
        F.col("/product_cnt").alias("product_cnt"),
        F.col("/search_value_sum").alias("search_value_sum"),
        F.col("/search_cnt").alias("search_cnt"),
    )
)
