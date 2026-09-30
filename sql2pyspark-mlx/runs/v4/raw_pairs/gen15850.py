from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.max("event_time").alias("latest_event_time"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
    )
    .orderBy("add_to_cart_count")
)
