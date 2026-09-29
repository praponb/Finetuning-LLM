from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.hour("started_at").between(12, 20) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.substring("device", 1, 2).alias("device_prefix"),
        F.upper("device").alias("device_upper"),
    )
)

cleaned_sessions = (
    cleaned
    .filter(F.col("started_at") >= F.lit("2023-02-27").cast("date"))
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        "device_prefix",
        "device_upper",
        F.year("started_at").alias("started_at_year"),
    )
)

filtered = (
    cleaned_sessions
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.floor("started_at_year").alias("started_at_year_whole"))
)

kept_keys = (
    filtered
    .select("customer_id", "country")
    .subtract(
        df_sessions
        .filter(F.col("country") == "SG")
        .select("customer_id", "country")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") <= F.col("s.started_at")), "left_semi")
)

remaining_keys = (
    not_flagged
    .select("customer_id", "country")
    .subtract(
        df_sessions
        .filter(F.col("country").isin(["DE", "UK", "TH"]))
        .select("customer_id", "country")
    )
)

kept_rows = (
    not_flagged
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.started_at")), "left_semi")
)

stats = (
    kept
    .rollup(F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.max(F.when(F.col("country") == "JP", F.col("started_at_year"))).alias("jp_max_started_at_year"),
        F.max("started_at").alias("ended_at"),
    )
)

result = (
    stats
    .select("de_rows", "sg_count", "ended_at")
    .orderBy("de_rows")
)
