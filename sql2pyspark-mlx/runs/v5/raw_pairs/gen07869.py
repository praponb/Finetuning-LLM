from pyspark.sql import functions as F

remaining_keys = (
    df_web_events
    .select("id", "page")
    .intersect(
        df_web_events
        .filter(F.col("session_id").isin([16, 26, 31]))
        .select("id", "page")
    )
)

reconciled = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .filter(F.col("b.device") == "desktop")
    .select("src.*", F.col("b.device"), F.col("b.browser"), F.col("b.country"))
)

enriched = (
    joined
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .select(
        "a.session_id",
        "a.event_time",
        "a.device",
        "a.browser",
        F.col("p.device").alias("session_device"),
    )
)

remaining_keys_2 = (
    enriched
    .select("session_id")
    .exceptAll(
        df_web_events
        .filter(F.col("event_type").isin(["click", "purchase"]))
        .select("session_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

joined_web_events = (
    kept_rows
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "inner")
    .select(
        "a.*",
        F.col("p.browser").alias("session_browser"),
        F.col("p.country"),
        F.col("p.device").alias("session_device_2"),
    )
)

with_dims = (
    joined_web_events
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .select(
        "src.session_id",
        "src.event_time",
        "src.browser",
        F.col("b.device").alias("session_device_3"),
    )
)

joined_web_events_2 = (
    with_dims
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "inner")
    .select("s.*", F.col("p2.browser").alias("session_browser"), F.col("p2.device"))
)

result = (
    joined_web_events_2
    .groupBy(F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        F.min("event_time").alias("first_seen"),
        F.count(F.when(F.col("session_device_3") == "mobile", 1)).alias("mobile_count"),
    )
    .orderBy(F.col("year_start").desc())
)
