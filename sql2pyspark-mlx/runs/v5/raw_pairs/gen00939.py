from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", "session_id")
    .pivot("page", ["/search", "/product"])
    .agg(F.max("duration_sec").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "event_type",
        "session_id",
        F.col("/search_amount").alias("search_amount"),
        F.col("/search_n").alias("search_n"),
        F.col("/product_amount").alias("product_amount"),
        F.col("/product_n").alias("product_n"),
    )
)
