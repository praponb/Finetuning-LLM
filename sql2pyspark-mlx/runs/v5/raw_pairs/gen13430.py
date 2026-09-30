from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("event_type") != "page_view")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.when(F.col("event_type").isin(["add_to_cart", "purchase"]), "Tier 1").otherwise("Other").alias("event_type_group"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

enriched = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.page",
        "s.event_time",
        "s.page_clean",
        F.col("p2.browser"),
        F.col("p2.device"),
    )
)

enriched_web_events = (
    enriched
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.session_id",
        "src.page",
        F.col("b.country"),
        F.col("b.device").alias("session_device"),
    )
)

with_dims = (
    enriched_web_events
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.country").alias("session_country"))
)

enriched_web_events_2 = (
    with_dims
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "inner")
    .filter(F.col("p2.country") == "DE")
    .select(
        "s.id",
        "s.session_id",
        "s.session_country",
        F.col("p2.country").alias("session_country_2"),
    )
)

result = (
    enriched_web_events_2
    .rollup("session_id")
    .agg(
        F.count(F.when(F.col("session_country_2") == "SG", 1)).alias("sg_count"),
        F.count(F.when(F.col("session_country") == "US", 1)).alias("us_count"),
    )
)
