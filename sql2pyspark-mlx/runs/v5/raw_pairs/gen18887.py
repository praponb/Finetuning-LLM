from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode_outer("context").alias("key", "value"),
    )
)

valid = (
    exploded
    .filter(F.col("log_ts") > F.to_date(F.lit("2025-06-01")))
    .select("*", F.trim("service").alias("clean_service"))
)

valid_app_logs = (
    valid
    .filter(F.col("key").isNotNull() & (F.col("log_ts") >= F.lit("2025-06-01").cast("date")))
    .withColumn("level_group", F.when(F.col("level").isin(["WARN", "DEBUG"]), "Group A").otherwise("Other"))
    .withColumn("resolved_key", F.coalesce("key", F.lit("UNKNOWN")))
)

base = (
    valid_app_logs
    .filter((F.col("key") == "trace") & (~F.col("key").eqNullSafe(F.lit("region"))))
    .select(
        "id",
        "host",
        "level",
        "key",
        "value",
        "clean_service",
        "level_group",
        "resolved_key",
        F.trim("service").alias("clean_service_2"),
        F.coalesce("key", F.lit("NONE")).alias("key_clean"),
    )
)

remaining_keys = (
    base
    .select("level", "id")
    .subtract(
        df_app_logs
        .filter(F.col("level").isin(["DEBUG", "INFO"]))
        .select("level", "id")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("key").isNotNull() & F.col("key").isin(["trace", "version"]))
    .select(
        "*",
        F.lower("key").alias("key_lower"),
        F.lower("host").alias("host_lower"),
        F.coalesce("key", F.lit("OTHER")).alias("resolved_key_2"),
    )
)

filtered = (
    cleaned
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.lower("key").alias("key_lower_2"),
        F.coalesce("key", F.lit("NONE")).alias("resolved_key_3"),
    )
)

daily = (
    filtered
    .groupBy("key", "level")
    .agg(
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.countDistinct("key").alias("distinct_keys"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_rows"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_count"),
    )
)

result = (
    daily
    .select(
        "key",
        "level",
        "host_list",
        "distinct_keys",
        "event_count",
        "h2_rows",
        "level_list",
        F.when(F.col("level").rlike("^(WARN|ERROR)$"), "OTHER_ZONE").when(F.col("level").rlike("^(INFO|DEBUG)$"), "CORE").otherwise("GROUP_A").alias("level_zone"),
    )
    .orderBy(F.col("h2_rows").desc_nulls_last())
)
