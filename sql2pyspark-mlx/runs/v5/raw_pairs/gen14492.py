from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("page").isin(["/checkout", "/product", "/cart"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)

enriched = (
    base
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "inner")
    .filter(F.col("p.browser") != "Firefox")
    .select(
        "a.id",
        "a.session_id",
        "a.event_type",
        F.col("p.browser"),
        F.col("p.device"),
        F.col("p.country"),
    )
)

base_web_events = (
    enriched
    .filter(~F.col("device").eqNullSafe(F.lit("tablet")))
    .select(
        "id",
        "session_id",
        "event_type",
        "browser",
        "device",
        "country",
        F.upper("event_type").alias("event_type_upper"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("resolved_browser"),
    )
)

enriched_web_events = (
    base_web_events
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .select(
        "src.*",
        F.col("d.browser").alias("session_browser"),
        F.col("d.device").alias("session_device"),
    )
)

joined = (
    enriched_web_events
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .select(
        "s.*",
        F.col("b.country").alias("session_country"),
        F.col("b.device").alias("session_device_2"),
        F.col("b.browser").alias("session_browser_2"),
    )
)

with_dims = (
    joined
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.session_id",
        "src.browser",
        "src.country",
        "src.resolved_browser",
        "src.session_country",
        "src.session_device_2",
        F.col("d.country").alias("session_country_2"),
    )
)

joined_web_events = (
    with_dims
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.country").alias("session_country_3"))
)

result = (
    joined_web_events
    .groupBy("country")
    .agg(
        F.sort_array(F.collect_set("session_country_3")).alias("session_country_3_list"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("country").desc())
)
