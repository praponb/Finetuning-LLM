from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

base = (
    flattened
    .filter(F.col("level") != "ERROR")
    .select(
        "id",
        "service",
        "level",
        "attr_key",
        "v",
        F.substring("attr_key", 1, 4).alias("attr_key_prefix"),
    )
)

base_app_logs = (
    base
    .filter(F.col("service") != "auth")
    .withColumn("service_lower", F.lower("service"))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

valid = (
    base_app_logs
    .filter(~F.col("service").isin(["search", "billing", "checkout"]))
    .select(
        "*",
        F.concat("attr_key", F.lit(" "), "service").alias("attr_key_service"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_filled"),
    )
)

cleaned = (
    valid
    .filter(F.col("service") == "search")
    .select("*", F.initcap(F.trim("service")).alias("service_title"))
)

filtered = (
    cleaned
    .filter(F.col("attr_key").eqNullSafe(F.lit("region")))
    .select(
        "id",
        "service",
        "level",
        "attr_key",
        "v",
        "service_lower",
        "attr_key_filled",
        "service_title",
        F.upper("service").alias("service_upper"),
        F.coalesce("attr_key", F.lit("NONE")).alias("attr_key_filled_2"),
    )
)

cleaned_app_logs = (
    filtered
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "service",
        "level",
        "attr_key",
        "service_lower",
        "attr_key_filled",
        "service_title",
        F.lower("level").alias("level_lower"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_filled_3"),
    )
)

stats = (
    cleaned_app_logs
    .groupBy("level")
    .agg(
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_rows"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_rows"),
        F.count_if(F.col("level") == "ERROR").alias("error_events"),
    )
    .filter(F.col("debug_rows") > 1)
)

result = (
    stats
    .select(
        "level",
        "debug_rows",
        F.when(F.col("level").rlike("^(ERROR|DEBUG)$"), "GROUP_B").when(F.col("level").rlike("^(INFO|WARN)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("level_zone"),
    )
    .orderBy("level_zone", "debug_rows")
)
