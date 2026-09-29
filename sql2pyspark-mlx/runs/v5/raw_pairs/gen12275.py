from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        F.concat("device", F.lit(" - "), "country").alias("device_country"),
        F.date_format("started_at", "MMM yyyy").alias("started_at_label"),
    )
)

kept_keys = (
    cleaned
    .select("browser")
    .subtract(
        df_sessions
        .filter(F.col("started_at") < "2023-08-01")
        .select("browser")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter((F.col("started_at") >= F.lit("2023-07-21").cast("date")) & F.col("browser").isNotNull())
    .select(
        "*",
        F.lower("device").alias("device_lower"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_filled"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_anti")
)

filtered_sessions = (
    not_flagged
    .filter((F.col("browser").eqNullSafe(F.lit("Safari"))) & (F.col("device") == "desktop"))
    .withColumn("started_at_period", F.date_trunc("WEEK", "started_at"))
)

remaining_keys = (
    filtered_sessions
    .select("id", "browser")
    .subtract(
        df_sessions
        .filter(F.col("device") == "desktop")
        .select("id", "browser")
    )
)

verified_sessions = (
    filtered_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

result = (
    verified_sessions
    .select(
        "browser",
        "device_country",
        "device_lower",
        "browser_filled",
        "started_at_period",
        F.when(F.col("browser").rlike("^(Edge|Chrome)$"), "EXTENDED").when(F.col("browser").rlike("^(Safari|Firefox)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("browser_zone"),
    )
    .orderBy(F.col("browser").desc())
)
