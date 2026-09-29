from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

valid = (
    flattened
    .filter(F.col("service") == "billing")
    .withColumn("host_title", F.initcap(F.trim("host")))
    .withColumn("resolved_k", F.coalesce("k", F.lit("OTHER")))
)

cleaned = (
    valid
    .filter(F.col("level") == "DEBUG")
    .select(
        "*",
        F.when(F.col("service").isin(["checkout", "auth"]), "Tier 1").otherwise("Other").alias("service_group"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("resolved_k_2"),
    )
)

valid_app_logs = (
    cleaned
    .filter(F.col("level") == "ERROR")
    .select(
        "id",
        "service",
        "host",
        "level",
        "k",
        "attr_value",
        "host_title",
        "service_group",
        "resolved_k_2",
        F.concat("service", F.lit(", "), "k").alias("service_k"),
    )
)

cleaned_app_logs = (
    valid_app_logs
    .filter(F.col("host") == "h2")
    .select(
        "*",
        F.concat("level", F.lit("/"), "k").alias("level_k"),
        F.coalesce("k", F.lit("N/A")).alias("k_filled"),
    )
)

cleaned_app_logs_2 = (
    cleaned_app_logs
    .filter(F.col("k").isNotNull() & (F.col("k") == "region"))
    .select(
        "*",
        F.upper("level").alias("level_upper"),
        F.initcap(F.trim("service")).alias("service_title"),
    )
)

base = (
    cleaned_app_logs_2
    .filter((F.col("k").eqNullSafe(F.lit("region"))) & ~F.col("level").isin(["DEBUG", "INFO"]))
    .select(
        "*",
        F.when(F.col("level").isin(["INFO", "ERROR"]), "Tier 1").otherwise("Other").alias("level_group"),
        F.coalesce("k", F.lit("NONE")).alias("k_clean"),
    )
)

result = (
    base
    .select(
        "service",
        "host",
        "level",
        "k",
        "resolved_k_2",
        "service_k",
        "level_k",
        "k_filled",
        "level_upper",
    )
    .orderBy(F.col("k").desc())
)
