from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_browser", "session_device_2").orderBy(F.col("duration_sec").desc_nulls_first())
w2 = Window.partitionBy("session_browser")

enriched = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "inner")
    .filter(~F.col("s.browser").isin(["Firefox", "Edge"]))
    .select("src.*", F.col("s.device"), F.col("s.country"))
)

base = (
    enriched
    .filter(F.col("country").isNotNull() & (F.col("country") != "TH"))
    .select(
        "*",
        F.concat("device", F.lit(" "), "country").alias("device_country"),
        F.substring("country", 1, 2).alias("country_prefix"),
    )
)

enriched_web_events = (
    base
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "inner")
    .select(
        "e.*",
        F.col("d.browser"),
        F.col("d.country").alias("session_country"),
        F.col("d.device").alias("session_device"),
    )
)

matched_keys = (
    enriched_web_events
    .select("session_id")
    .exceptAll(
        df_web_events
        .filter(F.col("event_type") != "search")
        .select("session_id")
    )
)

verified = (
    enriched_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "inner")
    .select(
        "s.session_id",
        "s.event_type",
        "s.event_time",
        "s.duration_sec",
        "s.device",
        "s.country",
        "s.device_country",
        "s.country_prefix",
        "s.session_country",
        "s.session_device",
        F.col("d.device").alias("session_device_2"),
        F.col("d.country").alias("session_country_2"),
        F.col("d.browser").alias("session_browser"),
    )
)

remaining_keys = (
    joined
    .select("event_type")
    .intersectAll(
        df_web_events
        .filter(F.col("page").isin(["/home", "/cart"]))
        .select("event_type")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "session_id",
        "duration_sec",
        "device",
        "country",
        "country_prefix",
        "session_country",
        "session_device",
        "session_country_2",
        "session_browser",
        F.cume_dist().over(w).alias("duration_sec_cume_dist"),
        F.max("duration_sec").over(w2).alias("session_browser_max_duration_sec"),
    )
)

result = (
    metrics
    .select(
        "session_id",
        "device",
        "country",
        "session_country",
        "session_device",
        "session_country_2",
        "session_browser",
        "session_browser_max_duration_sec",
        F.when(F.col("device").rlike("^(desktop|mobile)$"), "OTHER_ZONE").when(F.col("device").rlike("^(tablet)$"), "GROUP_B").otherwise("GROUP_A").alias("device_zone"),
    )
    .orderBy("session_country")
)
