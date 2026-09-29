from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .select(
        "*",
        F.length("page").alias("page_length"),
        F.coalesce("page", F.lit("OTHER")).alias("page_filled"),
    )
)

valid = (
    cleaned
    .filter(F.col("page").isin(["/checkout", "/cart", "/product"]) & (F.col("event_time") >= F.lit("2023-07-21 05:00:00").cast("timestamp")))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "page_length",
        "page_filled",
        (F.col("duration_sec") - F.col("page_length")).alias("duration_sec_minus_page_length"),
        (F.col("page_length") - F.col("duration_sec")).alias("page_length_minus_duration_sec"),
        F.coalesce("page", F.lit("NONE")).alias("page_clean"),
    )
)

enriched = (
    valid
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .select(
        "e.id",
        "e.session_id",
        "e.page_length",
        "e.duration_sec_minus_page_length",
        "e.page_clean",
        F.col("s.browser"),
    )
)

remaining_keys = (
    enriched
    .select("id")
    .intersect(
        df_web_events
        .filter(F.col("duration_sec").between(181, 374))
        .select("id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("duration_sec_minus_page_length") >= 87, "PREMIUM").otherwise("STANDARD").alias("duration_sec_minus_page_length_tier"),
    )
    .agg(
        F.percentile_approx("duration_sec_minus_page_length", 0.9).alias("duration_sec_minus_page_length_p90"),
        F.sum(F.when(F.col("browser") == "Firefox", F.col("duration_sec_minus_page_length"))).alias("firefox_total_duration_sec_minus_page_length"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
    )
    .orderBy("firefox_total_duration_sec_minus_page_length")
)
