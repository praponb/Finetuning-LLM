from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & F.hour("started_at").between(9, 13))
    .withColumn("device_upper", F.upper("device"))
    .withColumn("browser_clean", F.coalesce("browser", F.lit("UNKNOWN")))
)

matched_keys = (
    cleaned
    .select("country", "device")
    .subtract(
        df_sessions
        .filter(F.col("device") == "mobile")
        .select("country", "device")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter((F.col("device") == "mobile") & (F.col("started_at") >= F.lit("2023-08-02 17:00:00").cast("timestamp")))
    .withColumn("is_de", F.when(F.col("country") == "DE", 1).otherwise(0))
    .withColumn("country_filled", F.coalesce("country", F.lit("OTHER")))
)

eligible = (
    valid
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") <= F.col("s.started_at")), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.to_date(F.col("x.event_time")) >= "2024-11-01"), "left_anti")
)

result = (
    kept
    .select(
        "device",
        "browser",
        "started_at",
        "country",
        "browser_clean",
        "is_de",
        "country_filled",
    )
    .orderBy(F.col("started_at").asc_nulls_first())
)
