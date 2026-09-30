from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

flattened_attrs = (
    lines
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "attr_key",
        "v",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

remaining_keys = (
    flattened_attrs
    .select("id")
    .intersect(
        df_app_logs
        .filter(F.col("service").isin(["billing", "auth", "checkout"]))
        .select("id")
    )
)

kept_rows = (
    flattened_attrs
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("attr_key").isNotNull() & (F.col("latency_ms") < 1784))
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "attr_key",
        "v",
        "has_fault",
        "status_code_high",
        F.upper("level").alias("level_upper"),
        F.least("latency_ms", "has_fault").alias("least_latency_ms_has_fault"),
    )
)

valid = (
    base
    .filter(~F.col("attr_key").eqNullSafe(F.lit("region")))
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "attr_key",
        "status_code_high",
        "level_upper",
        "least_latency_ms_has_fault",
        F.when(F.col("service") == "checkout", 1).otherwise(0).alias("is_checkout"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_filled"),
    )
)

result = (
    valid
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "attr_key",
        "status_code_high",
        "level_upper",
        "least_latency_ms_has_fault",
        "is_checkout",
        F.when(F.col("attr_key").rlike("^(trace|version)$"), "OTHER_ZONE").when(F.col("attr_key").rlike("^(region)$"), "GROUP_A").otherwise("GROUP_B").alias("attr_key_zone"),
    )
    .orderBy(F.col("id").desc())
)
