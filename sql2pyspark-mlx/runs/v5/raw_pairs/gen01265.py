from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("page_event_type_key", F.concat_ws("-", "page", "event_type"))
    .withColumn("adjusted_duration_sec", F.round((F.col("duration_sec") * 0.85), 2))
)

enriched = (
    base
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "left")
    .filter(F.col("p2.device") != "desktop")
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.page",
        F.col("p2.browser"),
        F.col("p2.country"),
    )
)

joined = (
    enriched
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.country").alias("session_country"),
        F.col("p.browser").alias("session_browser"),
    )
)

enriched_web_events = (
    joined
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.session_id",
        "e.event_type",
        "e.page",
        F.col("p.device"),
        F.col("p.country").alias("session_country_2"),
        F.col("p.browser").alias("session_browser_2"),
    )
)

valid = (
    enriched_web_events
    .filter(F.col("device") != "desktop")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "device",
        "session_country_2",
        F.substring("device", 1, 3).alias("device_prefix"),
    )
)

result = (
    valid
    .groupBy("event_type")
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.count_if(F.col("device") == "tablet").alias("tablet_events"),
        F.count_if(F.col("page") == "/checkout").alias("checkout_events"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_rows"),
    )
    .filter(F.col("tablet_events") > 1)
    .orderBy(F.col("purchase_rows").asc_nulls_first())
)
