from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

cleaned = (
    flattened
    .filter(F.col("attr_key").isNotNull())
    .select(
        "service",
        "level",
        "attr_key",
        F.floor("status_code").alias("status_code_whole"),
    )
)

matched_keys = (
    cleaned
    .select("service", "level")
    .subtract(
        df_app_logs
        .filter(F.col("latency_ms").isNotNull())
        .select("service", "level")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

kept_keys = (
    reconciled
    .select("service")
    .subtract(
        df_app_logs
        .filter(F.col("host") == "h2")
        .select("service")
    )
)

reconciled_app_logs = (
    reconciled
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

cleaned_app_logs = (
    reconciled_app_logs
    .filter(F.col("attr_key").isNotNull())
    .withColumn("attr_key_prefix", F.substring("attr_key", 1, 3))
)

result = (
    cleaned_app_logs
    .select("service", "attr_key", "status_code_whole", "attr_key_prefix")
    .orderBy("service", "attr_key")
)
