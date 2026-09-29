from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") == 223) & F.col("page").isin(["/cart", "/search"]) & F.hour("event_time").between(12, 16))
    .groupBy("page")
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
    )
    .orderBy("page")
)
