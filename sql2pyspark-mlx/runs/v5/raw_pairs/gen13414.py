from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2024-05-01")
    .groupBy("event_type")
    .agg(
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
    )
    .filter(F.col("add_to_cart_count") > 5)
)
