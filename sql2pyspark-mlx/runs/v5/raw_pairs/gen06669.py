from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .pivot("page", ["/search", "/product", "/cart"])
    .agg(F.avg("duration_sec").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "session_id",
        F.col("/search_amount").alias("search_amount"),
        F.col("/search_cnt").alias("search_cnt"),
        F.col("/product_amount").alias("product_amount"),
        F.col("/product_cnt").alias("product_cnt"),
        F.col("/cart_amount").alias("cart_amount"),
        F.col("/cart_cnt").alias("cart_cnt"),
    )
)
