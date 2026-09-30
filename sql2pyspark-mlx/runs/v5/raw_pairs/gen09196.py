from pyspark.sql import functions as F

base = (
    df_sessions
    .filter(F.col("browser").isNotNull() & (F.to_date("started_at") >= "2025-10-01"))
    .withColumn("device_length", F.length("device"))
    .withColumn("device_lower", F.lower("device"))
)

filtered = (
    base
    .filter((~F.col("country").eqNullSafe(F.lit("JP"))) & F.col("browser").isNotNull())
    .select("id", "browser", "device_lower", F.upper("country").alias("country_upper"))
)

kept = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.page").isin(["/home", "/cart", "/checkout"])), "left_anti")
)

cleaned = (
    kept
    .filter(F.col("browser").isNotNull() & (~F.col("browser").eqNullSafe(F.lit("Safari"))))
    .withColumn("browser_prefix", F.substring("browser", 1, 4))
    .withColumn("browser_upper", F.upper("browser"))
    .withColumn("browser_clean", F.coalesce("browser", F.lit("OTHER")))
)

report = (
    cleaned
    .select(
        "id",
        "browser",
        "device_lower",
        "country_upper",
        "browser_prefix",
        F.when(F.col("browser").rlike("^(Edge|Safari)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("browser_zone"),
    )
    .orderBy(F.col("browser").desc(), F.col("browser_zone").desc())
)

result = (
    report
    .select("id", "browser", "browser_prefix", "browser_zone")
    .orderBy(F.col("browser_zone").desc_nulls_last(), F.col("browser").desc())
)
