from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("event_time") > F.to_date(F.lit("2023-06-05")))
    .select(
        "*",
        F.lower("page").alias("page_lower"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("resolved_page"),
    )
)

result = (
    cleaned
    .groupBy("page")
    .pivot("event_type", ["add_to_cart", "search", "page_view"])
    .agg(
        F.avg("duration_sec").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
)
