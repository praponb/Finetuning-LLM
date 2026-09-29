from pyspark.sql import functions as F

base = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-04-24 02:00:00").cast("timestamp")) & F.col("customer_id").isNotNull() & F.col("device").isin(["desktop", "mobile", "tablet"]))
    .select(
        "*",
        F.to_date("started_at").alias("started_at_date"),
        F.coalesce("country", F.lit("NONE")).alias("country_clean"),
    )
)

matched_keys = (
    base
    .select("id")
    .exceptAll(
        df_sessions
        .filter(F.col("customer_id").isNull())
        .select("id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_type") == "search"), "left_semi")
)

joined = (
    kept
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.city"), F.col("p.age"))
)

remaining_keys = (
    joined
    .select("browser", "country")
    .exceptAll(
        df_sessions
        .filter(F.col("device").isin(["desktop", "mobile", "tablet"]))
        .select("browser", "country")
    )
)

kept_rows_sessions = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    kept_rows_sessions
    .select(
        "device",
        "browser",
        "started_at",
        "country",
        "started_at_date",
        "country_clean",
        "age",
        F.when(F.col("browser").rlike("^(Chrome|Firefox)$"), "CORE").otherwise("PRIORITY").alias("browser_zone"),
    )
    .orderBy(F.col("started_at").desc())
)
