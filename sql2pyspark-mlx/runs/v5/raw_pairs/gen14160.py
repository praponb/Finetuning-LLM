from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-07-30").cast("date")) & F.col("session_id").isin([36, 40]))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        F.length("event_type").alias("event_type_length"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_clean"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("event_type_length") >= 75, "LARGE").otherwise("SMALL").alias("event_type_length_tier"),
    )
    .agg(
        F.min("event_time").alias("started_at"),
        F.count_if(F.col("page") == "/home").alias("home_events"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.avg("event_type_length").alias("avg_event_type_length"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
    )
)
