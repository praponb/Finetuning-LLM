from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2024-04-15 18:00:00").cast("timestamp")) & (F.col("event_type") == "click"))
    .select("*", F.concat("event_type", F.lit("/"), "page").alias("event_type_page"))
)

result = (
    filtered
    .groupBy("event_type")
    .pivot("page", ["/product", "/cart", "/checkout"])
    .agg(F.max("duration_sec"))
    .select(
        "event_type",
        F.col("/product").alias("product"),
        F.col("/cart").alias("cart"),
        F.col("/checkout").alias("checkout"),
    )
)
