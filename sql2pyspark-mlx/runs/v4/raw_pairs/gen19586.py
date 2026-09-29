from pyspark.sql import functions as F

remaining_keys = (
    df_web_events
    .select("id", "page")
    .subtract(
        df_web_events
        .filter(F.col("event_type").isin(["page_view", "purchase", "search"]))
        .select("id", "page")
    )
)

reconciled = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 426, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["event_type", "duration_sec_tier"], ["event_type"], []],
        "event_type",
        "duration_sec_tier",
    )
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
        F.grouping_id().alias("grouping_id"),
    )
)
