from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
    .filter(F.col("pct_search") > 30)
    .select("event_type", "add_to_cart_count", "pct_search")
    .orderBy("event_type", F.col("add_to_cart_count").desc())
    .limit(3)
)
