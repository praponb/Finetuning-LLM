from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        F.explode_outer("context").alias("key", "value"),
    )
)

filtered = (
    lines
    .filter(F.col("key").isNotNull() & (F.col("key") == "region"))
    .select(
        "*",
        F.length("key").alias("key_length"),
        F.coalesce("key", F.lit("N/A")).alias("key_clean"),
    )
)

base = (
    filtered
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("key").isNotNull())
    .select(
        "id",
        "log_ts",
        "level",
        "key",
        "value",
        "key_clean",
        F.concat("service", F.lit(", "), "level").alias("service_level"),
        F.concat("key", F.lit(" - "), "service").alias("key_service"),
        F.coalesce("key", F.lit("N/A")).alias("resolved_key"),
    )
)

valid = (
    base
    .filter((F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 15:00:00"))) & (F.col("level") != "DEBUG"))
    .withColumn("key_level_key", F.concat_ws("/", "key", "level"))
    .withColumn("resolved_key_2", F.coalesce("key", F.lit("NONE")))
)

filtered_app_logs = (
    valid
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("key").isNotNull() & F.hour("log_ts").between(6, 13))
    .select("*", F.concat("level", F.lit("/"), "key").alias("level_key"))
)

result = (
    filtered_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "key",
        "value",
        "key_service",
        "resolved_key",
        "key_level_key",
        "resolved_key_2",
        F.when(F.col("level").rlike("^(INFO|DEBUG)$"), "EXTENDED").when(F.col("level").rlike("^(WARN|ERROR)$"), "PRIORITY").otherwise("GROUP_A").alias("level_zone"),
    )
    .orderBy("log_ts")
)
