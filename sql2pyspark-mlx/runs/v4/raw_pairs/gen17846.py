from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-04-03 23:00:00").cast("timestamp")) & (F.col("event_type") != "page_view"))
    .select(
        "*",
        F.to_date("event_time").alias("event_time_date"),
        F.round("duration_sec", 0).alias("duration_sec_rounded"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("duration_sec") >= 525, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.count_if(F.col("event_type") == "add_to_cart").alias("add_to_cart_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_rows"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.min("event_time").alias("started_at"),
    )
    .filter(F.col("purchase_rows") >= 1)
)
