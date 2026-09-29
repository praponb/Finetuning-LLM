from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 499, "PREMIUM").otherwise("STANDARD"))
    .cube("duration_sec_tier")
    .agg(
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_rows"),
        F.count("*").alias("event_count"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("add_to_cart_count") < 1000)
    .orderBy(F.col("duration_sec_tier").desc())
)
