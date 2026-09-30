from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

filtered = (
    flattened
    .filter(F.col("attr_key").isNotNull() & ~F.col("host").isin(["h2", "h4", "h1"]))
    .select(
        "id",
        "service",
        "value",
        F.when(F.col("attr_key").isin(["trace", "version"]), "Core").otherwise("Other").alias("attr_key_group"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_filled"),
    )
)

cleaned = (
    filtered
    .filter(F.col("service") == "checkout")
    .select(
        "id",
        "service",
        "attr_key_group",
        F.length("service").alias("service_length"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "service",
        "attr_key_group",
        "service_length",
        F.when(F.col("service").rlike("^(auth|billing)$"), "CORE").otherwise("PRIORITY").alias("service_zone"),
    )
    .orderBy("id", F.col("service").desc())
)
