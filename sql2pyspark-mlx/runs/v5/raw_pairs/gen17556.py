from pyspark.sql import functions as F

duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

valid = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-05-30").cast("date"))
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.upper("event_type").alias("event_type_upper"),
        F.coalesce("page", F.lit("N/A")).alias("page_clean"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.browser"), F.col("p.country"), F.col("p.device"))
)

joined = (
    enriched
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.duration_sec",
        "s.duration_sec_filled",
        "s.event_type_upper",
        "s.page_clean",
        "s.browser",
        "s.country",
        F.col("d.device").alias("session_device"),
        F.col("d.browser").alias("session_browser"),
    )
)

enriched_web_events = (
    joined
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.browser").alias("session_browser_2"),
        F.col("p.country").alias("session_country"),
        F.col("p.device"),
    )
)

result = (
    enriched_web_events
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "duration_sec",
        "duration_sec_filled",
        "event_type_upper",
        "country",
        "session_device",
        "session_browser_2",
        "session_country",
        F.when(F.col("country").rlike("^(TH|DE)$"), "GROUP_B").when(F.col("country").rlike("^(US|SG)$"), "PRIORITY").otherwise("GROUP_A").alias("country_zone"),
        F.col("max_web_event_duration_sec"),
    )
    .orderBy(F.col("session_browser_2").desc())
)
