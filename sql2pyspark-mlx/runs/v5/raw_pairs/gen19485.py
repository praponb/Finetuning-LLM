from pyspark.sql import functions as F

with_dims = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.session_id",
        "e.event_time",
        F.col("d.browser"),
        F.col("d.device"),
    )
)

kept_keys = (
    with_dims
    .select("id")
    .intersect(
        df_web_events
        .filter(F.to_date("event_time") >= "2023-05-01")
        .select("id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter((F.col("event_time") >= F.lit("2023-06-10").cast("date")) & F.col("device").isNotNull())
    .select(
        "session_id",
        "event_time",
        "browser",
        "device",
        F.coalesce("browser", F.lit("Unknown")).alias("browser_filled"),
        F.coalesce("device", F.lit("N/A")).alias("device_clean"),
    )
)

enriched = (
    base
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .filter(F.col("b.customer_id") == 28)
    .select(
        "e.*",
        F.col("b.browser").alias("session_browser"),
        F.col("b.device").alias("session_device"),
        F.col("b.country"),
    )
)

result = (
    enriched
    .select(
        "session_id",
        "event_time",
        "browser",
        "device",
        "browser_filled",
        "device_clean",
        "session_device",
        "country",
        F.when(F.col("device").rlike("^(desktop|mobile)$"), "EXTENDED").otherwise("GROUP_B").alias("device_zone"),
    )
)
