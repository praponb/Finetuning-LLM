from pyspark.sql import functions as F

kept_keys = (
    df_web_events
    .select("event_type")
    .intersectAll(
        df_web_events
        .filter(F.col("event_type") == "click")
        .select("event_type")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

matched_keys = (
    verified
    .select("page", "id")
    .subtract(
        df_web_events
        .filter(F.col("page") == "/home")
        .select("page", "id")
    )
)

verified_web_events = (
    verified
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified_web_events
    .filter(F.col("page").isNotNull() & F.col("event_type").isin(["purchase", "add_to_cart"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.concat("event_type", F.lit(" - "), "page").alias("event_type_page"),
    )
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .select("a.session_id", "a.event_type_page", F.col("p.browser"), F.col("p.device"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .select(
        "e.*",
        F.col("s.country"),
        F.col("s.browser").alias("session_browser"),
        F.col("s.device").alias("session_device"),
    )
)

with_dims_web_events = (
    enriched
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.customer_id").isin([22, 25, 36]))
    .select(
        "src.session_id",
        "src.country",
        "src.session_browser",
        "src.session_device",
        F.col("d.browser").alias("session_browser_2"),
        F.col("d.country").alias("session_country"),
    )
)

joined = (
    with_dims_web_events
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.country").alias("session_country_2"))
)

result = (
    joined
    .groupingSets(
        [["session_country_2", "session_browser"], ["session_country_2"], []],
        "session_country_2",
        "session_browser",
    )
    .agg(
        F.count_if(F.col("session_device") == "desktop").alias("desktop_events"),
        F.sort_array(F.collect_set("session_country_2")).alias("session_country_2_list"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_rows"),
        F.count_if(F.col("session_browser") == "Edge").alias("edge_events"),
        F.grouping("session_country_2").alias("grouping_id"),
    )
)
