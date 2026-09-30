from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & F.col("country").isin(["SG", "UK", "JP"]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.concat("country", F.lit("/"), "device").alias("country_device"),
        F.coalesce("browser", F.lit("N/A")).alias("resolved_browser"),
    )
)

valid = (
    cleaned
    .filter(F.col("device").isin(["desktop", "tablet", "mobile"]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        "country_device",
        "resolved_browser",
        F.initcap(F.trim("country")).alias("country_title"),
    )
)

cleaned_sessions = (
    valid
    .filter(F.col("browser").isNotNull() & (F.col("device") == "tablet"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country_device",
        "resolved_browser",
        "country_title",
        F.length("browser").alias("browser_length"),
        F.concat("browser", F.lit("/"), "country").alias("browser_country"),
    )
)

kept_keys = (
    cleaned_sessions
    .select("id", "customer_id")
    .intersect(
        df_sessions
        .filter(F.col("browser") == "Edge")
        .select("id", "customer_id")
    )
)

verified = (
    cleaned_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.duration_sec").between(174, 202)), "left_anti")
)

enriched = (
    eligible
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.segment"), F.col("p.city"))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("browser_length") >= 73, "LARGE").otherwise("SMALL").alias("browser_length_tier"),
    )
    .agg(
        F.round(F.sum("browser_length") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("browser_length_per_london"),
        F.round(F.avg("browser_length"), 2).alias("avg_browser_length"),
        F.round(F.sum("browser_length") / F.nullif(F.count(F.when(F.col("device") == "desktop", 1)), F.lit(0)), 2).alias("browser_length_per_desktop"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.percentile_approx("browser_length", 0.95).alias("browser_length_p95"),
    )
    .orderBy(F.col("browser_length_per_london").desc_nulls_last())
)
