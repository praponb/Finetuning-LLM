from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-09-09 09:00:00").cast("timestamp")) & (F.col("page") != "/checkout"))
    .select(
        "*",
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
        F.coalesce("page", F.lit("N/A")).alias("page_clean"),
    )
)

filtered_web_events = (
    filtered
    .filter((F.col("event_time") > F.lit("2023-09-04").cast("date")) & (F.col("event_type") == "page_view"))
    .select(
        "*",
        F.when(F.col("page").isin(["/cart", "/product"]), "Priority").otherwise("Other").alias("page_group"),
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean_2"),
    )
)

matched_keys = (
    filtered_web_events
    .select("id", "page")
    .exceptAll(
        df_web_events
        .filter(F.col("event_type") != "add_to_cart")
        .select("id", "page")
    )
)

reconciled = (
    filtered_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.session_id",
        "a.event_type",
        "a.duration_sec",
        "a.page_group",
        "a.days_since_event_time",
        "a.page_clean_2",
        F.col("d.device"),
    )
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("duration_sec") >= 401, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_rows"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_rows"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_rows_2"),
    )
    .filter(F.col("mobile_rows_2") > 1)
    .orderBy(F.col("purchase_rows").desc_nulls_first())
)
