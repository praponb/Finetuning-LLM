from pyspark.sql import functions as F

with_dims = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "inner")
    .select(
        "src.id",
        "src.session_id",
        "src.page",
        "src.duration_sec",
        F.col("s.browser"),
        F.col("s.device"),
        F.col("s.country"),
    )
)

with_dims_web_events = (
    with_dims
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.customer_id").isin([2, 18, 19]))
    .select(
        "src.id",
        "src.session_id",
        "src.duration_sec",
        "src.device",
        "src.country",
        F.col("d.country").alias("session_country"),
        F.col("d.browser").alias("session_browser"),
        F.col("d.device").alias("session_device"),
    )
)

with_dims_web_events_2 = (
    with_dims_web_events
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.session_id",
        "src.device",
        "src.country",
        "src.session_country",
        F.col("b.browser"),
    )
)

valid = (
    with_dims_web_events_2
    .filter(F.col("session_country").isNotNull())
    .select(
        "id",
        "session_id",
        "device",
        "country",
        "session_country",
        "browser",
        F.trim("device").alias("clean_device"),
        F.coalesce("device", F.lit("Unknown")).alias("device_filled"),
    )
)

result = (
    valid
    .groupBy("browser")
    .agg(
        F.countDistinct("device").alias("distinct_devices"),
        F.count("*").alias("event_count"),
        F.count_if(F.col("session_country") == "UK").alias("uk_events"),
        F.size(F.collect_set("device")).alias("num_devices"),
    )
    .orderBy("browser")
)
