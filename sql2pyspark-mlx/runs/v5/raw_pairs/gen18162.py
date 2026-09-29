from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.to_date(F.lit("2023-10-01"))) & F.col("session_id").isin([21, 24, 29]))
    .select("*", F.lower("page").alias("page_lower"))
)

base = (
    cleaned
    .filter((F.col("event_time") >= F.to_timestamp(F.lit("2023-04-23 18:00:00"))) & F.col("page").isin(["/checkout", "/home"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page_lower",
        F.round((F.col("duration_sec") * 0.9), 2).alias("duration_sec_scaled"),
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
        F.coalesce("page", F.lit("OTHER")).alias("resolved_page"),
    )
)

enriched = (
    base
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.country"), F.col("b.browser"), F.col("b.device"))
)

enriched_web_events = (
    enriched
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "inner")
    .select(
        "s.*",
        F.col("b.country").alias("session_country"),
        F.col("b.browser").alias("session_browser"),
    )
)

result = (
    enriched_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page_lower",
        "duration_sec_scaled",
        "duration_sec_rounded",
        "country",
        "browser",
        "device",
    )
)
