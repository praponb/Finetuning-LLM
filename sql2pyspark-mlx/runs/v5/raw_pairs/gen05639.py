from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("level_length").asc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

base = (
    lines
    .filter(F.col("host") == "h2")
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        "attr_key",
        "value",
        F.when(F.col("status_code") >= 437, "High").when(F.col("status_code") >= 298, "Medium").otherwise("Low").alias("status_code_tier"),
        F.coalesce("attr_key", F.lit("NONE")).alias("attr_key_filled"),
    )
)

base_app_logs = (
    base
    .filter(F.col("attr_key").isNotNull())
    .withColumn("attr_key_title", F.initcap(F.trim("attr_key")))
)

filtered = (
    base_app_logs
    .filter(F.col("attr_key").isNotNull() & (F.col("level") == "ERROR"))
    .select(
        "id",
        "service",
        "attr_key",
        "value",
        "status_code_tier",
        "attr_key_filled",
        "attr_key_title",
        F.initcap(F.trim("level")).alias("level_title"),
        F.length("level").alias("level_length"),
    )
)

valid = (
    filtered
    .filter(F.col("service") == "auth")
    .withColumn("level_length_str", F.col("level_length").cast("string"))
    .withColumn("attr_key_group", F.when(F.col("attr_key").isin(["trace", "version"]), "Core").otherwise("Other"))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("OTHER")))
)

metrics = (
    valid
    .select(
        "service",
        "attr_key",
        "value",
        "attr_key_filled",
        "attr_key_title",
        "level_title",
        "level_length",
        "attr_key_group",
        "attr_key_clean",
        F.cume_dist().over(w).alias("level_length_cume_dist"),
    )
)

matched_keys = (
    metrics
    .select("service")
    .subtract(
        df_app_logs
        .filter(F.col("service") == "checkout")
        .select("service")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

stats = (
    verified
    .withColumn("level_length_cume_dist_tier", F.when(F.col("level_length_cume_dist") >= 0.63, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["level_length_cume_dist_tier", "service", "attr_key"], ["level_length_cume_dist_tier", "service"]],
        "level_length_cume_dist_tier",
        "service",
        "attr_key",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("attr_key") == "version", 1)).alias("version_rows"),
        F.round(F.avg("level_length_cume_dist"), 2).alias("avg_level_length_cume_dist"),
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_rows"),
    )
)

result = (
    stats
    .select(
        "level_length_cume_dist_tier",
        "service",
        "row_count",
        "version_rows",
        "avg_level_length_cume_dist",
    )
    .orderBy("avg_level_length_cume_dist")
)
