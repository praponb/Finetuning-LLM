from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

valid = (
    flattened
    .filter(F.col("attr_key").isNotNull() & F.col("level").isin(["INFO", "WARN", "DEBUG"]))
    .select(
        "*",
        F.substring("level", 1, 2).alias("level_prefix"),
        F.coalesce("attr_key", F.lit("NONE")).alias("attr_key_clean"),
    )
)

base = (
    valid
    .filter(F.col("attr_key").isin(["version", "trace"]))
    .select(
        "id",
        "service",
        "attr_key",
        "level_prefix",
        "attr_key_clean",
        F.trim("attr_key").alias("clean_attr_key"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_filled"),
    )
)

cleaned = (
    base
    .filter(F.col("attr_key").isNotNull() & (F.col("service") != "auth"))
    .select(
        "id",
        "service",
        "attr_key",
        "level_prefix",
        "attr_key_clean",
        "clean_attr_key",
        F.concat("service", F.lit(" "), "attr_key").alias("service_attr_key"),
        F.length("attr_key").alias("attr_key_length"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "service",
        "attr_key",
        "level_prefix",
        "attr_key_clean",
        "clean_attr_key",
        "service_attr_key",
        "attr_key_length",
    )
    .orderBy(F.col("attr_key_length").desc_nulls_first())
)
