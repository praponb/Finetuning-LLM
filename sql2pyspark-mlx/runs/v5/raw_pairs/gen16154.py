from pyspark.sql import functions as F

enriched = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.session_id",
        "a.event_type",
        "a.page",
        F.col("d.country"),
        F.col("d.device"),
    )
)

remaining_keys = (
    enriched
    .select("event_type", "session_id")
    .intersect(
        df_web_events
        .filter(F.col("session_id").isin([28, 29, 32, 36]))
        .select("event_type", "session_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("device").isNotNull() & (F.col("device") != "desktop"))
    .select(
        "*",
        F.when(F.col("event_type").isin(["purchase", "click"]), "Core").otherwise("Other").alias("event_type_group"),
        F.concat("page", F.lit(" "), "event_type").alias("page_event_type"),
        F.coalesce("country", F.lit("N/A")).alias("resolved_country"),
    )
)

matched_keys = (
    cleaned
    .select("page")
    .subtract(
        df_web_events
        .filter(F.col("event_type") == "add_to_cart")
        .select("page")
    )
)

kept_rows_web_events = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

result = (
    kept_rows_web_events
    .rollup("device")
    .agg(
        F.size(F.collect_set("device")).alias("num_devices"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_rows"),
        F.countDistinct("device").alias("distinct_devices"),
        F.countDistinct("page").alias("distinct_pages"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
    )
)
