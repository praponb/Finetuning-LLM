from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
    .filter(F.col("pct_search") >= 34)
    .orderBy("add_to_cart_count", F.col("pct_search").desc())
)
