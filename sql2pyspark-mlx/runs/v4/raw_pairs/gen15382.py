from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "host", "level", F.explode("context").alias("key", "attr_value"))
)

cleaned = (
    exploded
    .filter(F.col("key").isNotNull() & (F.col("host") == "h3"))
    .withColumn("level_key_key", F.concat_ws(",", "level", "key"))
)

valid = (
    cleaned
    .filter(F.col("key").isNotNull() & F.col("key").isin(["region", "trace", "version"]))
    .withColumn("host_title", F.initcap(F.trim("host")))
    .withColumn("key_host", F.concat("key", F.lit("/"), "host"))
    .withColumn("key_filled", F.coalesce("key", F.lit("OTHER")))
)

filtered = (
    valid
    .filter(F.col("key") != "version")
    .select(
        "id",
        "key",
        "level_key_key",
        "host_title",
        "key_filled",
        F.lower("host").alias("host_lower"),
        F.upper("key").alias("key_upper"),
    )
)

summary = (
    filtered
    .groupBy("key")
    .agg(
        F.count(F.when(F.col("key") == "version", 1)).alias("version_rows"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
    )
    .filter(F.col("version_rows") > 1)
)

result = (
    summary
    .select(
        "key",
        "version_rows",
        "key_list",
        F.when(F.col("key").rlike("^(trace|version)$"), "OTHER_ZONE").when(F.col("key").rlike("^(region)$"), "PRIORITY").otherwise("GROUP_A").alias("key_zone"),
    )
    .orderBy("key")
)
