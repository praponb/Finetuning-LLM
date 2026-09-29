from pyspark.sql import functions as F

valid = (
    df_web_events
    .filter((~F.col("page").eqNullSafe(F.lit("/search"))) & (F.col("page") != "/product"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        F.lower("event_type").alias("event_type_lower"),
    )
)

joined = (
    valid
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.session_id",
        "a.event_type",
        "a.event_type_lower",
        F.col("d.country"),
        F.col("d.device"),
    )
)

joined_web_events = (
    joined
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "inner")
    .filter(~F.col("s.device").isin(["mobile", "tablet"]))
    .select(
        "a.id",
        "a.session_id",
        "a.device",
        F.col("s.country").alias("session_country"),
        F.col("s.browser"),
    )
)

kept_keys = (
    joined_web_events
    .select("id", "session_id")
    .subtract(
        df_web_events
        .filter(F.col("event_time") < "2023-05-01")
        .select("id", "session_id")
    )
)

verified = (
    joined_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

valid_web_events = (
    verified
    .filter(F.col("browser") == "Edge")
    .withColumn("browser_title", F.initcap(F.trim("browser")))
    .withColumn("device_lower", F.lower("device"))
    .withColumn("resolved_session_country", F.coalesce("session_country", F.lit("NONE")))
)

result = (
    valid_web_events
    .groupingSets([["browser", "session_id"], ["browser"], []], "browser", "session_id")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("device").alias("distinct_devices"),
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_count"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("firefox_count") >= 1)
    .orderBy(F.col("session_id").desc_nulls_first())
)
