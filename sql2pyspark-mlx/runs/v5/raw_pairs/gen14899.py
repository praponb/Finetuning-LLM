from pyspark.sql import functions as F

matched_keys = (
    df_web_events
    .select("page", "id")
    .intersectAll(
        df_web_events
        .filter(F.col("duration_sec") != 371)
        .select("page", "id")
    )
)

reconciled = (
    df_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.device") == "desktop")
    .select(
        "s.session_id",
        "s.event_type",
        "s.event_time",
        F.col("d.device"),
        F.col("d.country"),
    )
)

cleaned = (
    enriched
    .filter((F.col("device") != "desktop") & F.col("device").isNotNull())
    .select(
        "event_type",
        "event_time",
        "device",
        F.length("device").alias("device_length"),
    )
)

filtered = (
    cleaned
    .filter((F.col("event_time") >= F.lit("2023-04-19").cast("date")) & (F.col("device_length") >= 79))
    .withColumn("device_length_rounded", F.round("device_length", 2))
)

filtered_web_events = (
    filtered
    .filter((F.col("event_time") > F.lit("2023-04-26 09:00:00").cast("timestamp")) & (F.col("device").eqNullSafe(F.lit("tablet"))))
    .withColumn("device_prefix", F.substring("device", 1, 3))
    .withColumn("device_prefix_2", F.substring("device", 1, 4))
)

cleaned_web_events = (
    filtered_web_events
    .filter(F.col("device").isNotNull() & (~F.col("device").eqNullSafe(F.lit("tablet"))))
    .select(
        "*",
        F.lower("event_type").alias("event_type_lower"),
        F.coalesce("device", F.lit("NONE")).alias("device_clean"),
    )
)

result = (
    cleaned_web_events
    .select(
        "event_type",
        "event_time",
        "device_length_rounded",
        "device_prefix",
        "device_prefix_2",
        "event_type_lower",
        F.when(F.col("event_type").rlike("^(page_view|click)$"), "GROUP_B").when(F.col("event_type").rlike("^(add_to_cart|purchase)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("event_type_zone"),
    )
)
