from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-10-27").cast("date"))
    .select(
        "*",
        F.when(F.col("page").isin(["/cart", "/checkout"]), "Core").otherwise("Other").alias("page_group"),
        F.coalesce("page", F.lit("NONE")).alias("page_filled"),
    )
)

with_dims = (
    filtered
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.device"), F.col("p.country"), F.col("p.browser"))
)

enriched = (
    with_dims
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .select(
        "src.*",
        F.col("b.browser").alias("session_browser"),
        F.col("b.country").alias("session_country"),
    )
)

matched_keys = (
    enriched
    .select("page", "event_type")
    .intersectAll(
        df_web_events
        .filter(F.col("page") == "/home")
        .select("page", "event_type")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

with_dims_web_events = (
    verified
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "inner")
    .select(
        "e.*",
        F.col("s.device").alias("session_device"),
        F.col("s.browser").alias("session_browser_2"),
        F.col("s.country").alias("session_country_2"),
    )
)

joined = (
    with_dims_web_events
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "inner")
    .select(
        "e.*",
        F.col("d.browser").alias("session_browser_3"),
        F.col("d.country").alias("session_country_3"),
        F.col("d.device").alias("session_device_2"),
    )
)

enriched_web_events = (
    joined
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .select(
        "src.*",
        F.col("b.device").alias("session_device_3"),
        F.col("b.browser").alias("session_browser_4"),
    )
)

result = (
    enriched_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "page_group",
        "page_filled",
        "country",
        "browser",
        F.when(F.col("country").rlike("^(SG|US)$"), "GROUP_B").otherwise("PRIORITY").alias("country_zone"),
    )
    .orderBy(F.col("country").asc_nulls_first())
)
