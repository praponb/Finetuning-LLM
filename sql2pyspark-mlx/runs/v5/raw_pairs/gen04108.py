from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "host", "level", F.explode("context").alias("attr_key", "v"))
)

kept_keys = (
    lines
    .select("level", "id")
    .subtract(
        df_app_logs
        .filter(F.col("latency_ms") != 1911)
        .select("level", "id")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("attr_key").isNotNull())
    .withColumn("attr_key_group", F.when(F.col("attr_key").isin(["trace", "version"]), "Core").otherwise("Other"))
    .withColumn("host_upper", F.upper("host"))
)

valid_app_logs = (
    valid
    .filter(F.col("host") == "h4")
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("Unknown")))
    .withColumn("level_prefix", F.substring("level", 1, 4))
)

filtered = (
    valid_app_logs
    .filter(F.col("attr_key").isNotNull() & (F.col("host") != "h4"))
    .select(
        "id",
        "host",
        "v",
        "attr_key_group",
        "host_upper",
        "attr_key_filled",
        "level_prefix",
        F.concat_ws("-", "level", "attr_key").alias("level_attr_key_key"),
        F.concat("host", F.lit(" - "), "attr_key").alias("host_attr_key"),
        F.coalesce("attr_key", F.lit("NONE")).alias("attr_key_filled_2"),
    )
)

cleaned = (
    filtered
    .filter(F.col("host") == "h2")
    .select(
        "*",
        F.when(F.col("host").isin(["h3", "h4"]), "Core").otherwise("Other").alias("host_group"),
    )
)

filtered_app_logs = (
    cleaned
    .filter(F.col("host").isin(["h2", "h3"]))
    .select(
        "id",
        "host",
        "v",
        "attr_key_group",
        "attr_key_filled",
        "level_prefix",
        "host_attr_key",
        "attr_key_filled_2",
        "host_group",
        F.trim("host").alias("clean_host"),
    )
)

result = (
    filtered_app_logs
    .select(
        "id",
        "host",
        "v",
        "attr_key_group",
        "level_prefix",
        "host_attr_key",
        "clean_host",
    )
    .orderBy(F.col("host").asc_nulls_first(), "id")
)
