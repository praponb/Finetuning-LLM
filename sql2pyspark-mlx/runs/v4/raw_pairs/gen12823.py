from pyspark.sql import functions as F

enriched = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.device"))
)

joined = (
    enriched
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.device").alias("session_device"), F.col("d.browser"))
)

cleaned = (
    joined
    .filter(F.col("session_device") == "tablet")
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "device",
        "session_device",
        "browser",
        F.substring("device", 1, 3).alias("device_prefix"),
        F.to_date("event_time").alias("event_time_date"),
    )
)

result = (
    cleaned
    .withColumn("quarter_start", F.date_trunc("QUARTER", "event_time_date"))
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 470, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["quarter_start", "duration_sec_tier", "page"], ["quarter_start", "duration_sec_tier"]],
        "quarter_start",
        "duration_sec_tier",
        "page",
    )
    .agg(
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_rows"),
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_count"),
        F.sum(F.when(F.col("browser") == "Chrome", F.col("duration_sec")).otherwise(0)).alias("chrome_duration_sec"),
    )
    .orderBy(F.col("chrome_rows").desc_nulls_last())
)
