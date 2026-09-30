from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.to_date(F.lit("2024-01-19"))) & F.col("event_type").isin(["page_view", "click", "add_to_cart"]))
    .select(
        "*",
        F.when(F.col("duration_sec") >= 407, "High").when(F.col("duration_sec") >= 215, "Medium").otherwise("Low").alias("duration_sec_level"),
        F.concat_ws("-", "page", "event_type").alias("page_event_type_key"),
    )
)

base = (
    cleaned
    .filter((F.col("event_time") >= F.lit("2023-06-06").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        F.trim("page").alias("clean_page"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
    )
)

enriched = (
    base
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "inner")
    .select("a.*", F.col("s.country"), F.col("s.device"), F.col("s.browser"))
)

result = (
    enriched
    .groupingSets([["country", "session_id"], ["country"]], "country", "session_id")
    .agg(
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.countDistinct("page").alias("distinct_pages"),
    )
    .orderBy(F.col("distinct_pages").desc_nulls_first())
)
