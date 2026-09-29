from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.initcap(F.trim("device")).alias("device_title"),
        F.coalesce("country", F.lit("N/A")).alias("resolved_country"),
    )
)

filtered_sessions = (
    filtered
    .filter((F.col("started_at") >= F.lit("2023-09-02 05:00:00").cast("timestamp")) & F.col("country").isNotNull())
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        "device_title",
        F.concat_ws(",", "browser", "country").alias("browser_country_key"),
    )
)

remaining_keys = (
    filtered_sessions
    .select("browser", "id")
    .subtract(
        df_sessions
        .filter(F.col("country") == "TH")
        .select("browser", "id")
    )
)

kept_rows = (
    filtered_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter((F.col("started_at") >= F.to_timestamp(F.lit("2023-08-22 08:00:00"))) & F.col("country").isNotNull())
    .withColumn("browser_country_key_2", F.concat_ws("-", "browser", "country"))
    .withColumn("started_at_label", F.date_format("started_at", "yyyy-MM-dd"))
    .withColumn("country_filled", F.coalesce("country", F.lit("UNKNOWN")))
)

cleaned_sessions = (
    cleaned
    .filter(F.col("started_at") > F.to_date(F.lit("2023-03-31")))
    .select(
        "*",
        F.concat("browser", F.lit(" - "), "country").alias("browser_country"),
        F.when(F.col("browser").isin(["Firefox", "Chrome"]), "Core").otherwise("Other").alias("browser_group"),
    )
)

filtered_sessions_2 = (
    cleaned_sessions
    .filter((F.col("browser") == "Edge") & (F.col("browser").eqNullSafe(F.lit("Chrome"))))
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        "device_title",
        "browser_country_key",
        "browser_country_key_2",
        "started_at_label",
        "country_filled",
        "browser_country",
        "browser_group",
        F.date_trunc("week", "started_at").alias("started_at_period"),
        F.substring("country", 1, 3).alias("country_prefix"),
    )
)

cleaned_sessions_2 = (
    filtered_sessions_2
    .filter((F.col("started_at") >= F.lit("2023-03-28 20:00:00").cast("timestamp")) & (F.col("browser") != "Safari"))
    .select(
        "*",
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label_2"),
        F.upper("country").alias("country_upper"),
    )
)

report = (
    cleaned_sessions_2
    .select(
        "browser",
        "started_at",
        "country",
        "device_title",
        "browser_country_key",
        "browser_country_key_2",
        "started_at_label",
        "browser_country",
        "started_at_label_2",
        F.when(F.col("country").rlike("^(UK|DE)$"), "GROUP_A").when(F.col("country").rlike("^(TH|US)$"), "CORE").otherwise("GROUP_B").alias("country_zone"),
    )
)

result = (
    report
    .select(
        "browser",
        "started_at",
        "device_title",
        "browser_country_key",
        "browser_country_key_2",
        "started_at_label",
        "started_at_label_2",
        "country_zone",
    )
    .orderBy(F.col("started_at").desc_nulls_first(), F.col("country_zone").desc())
)
