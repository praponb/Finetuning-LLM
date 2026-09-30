from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "host", "log_ts", "level", F.explode("context").alias("key", "value"))
)

kept_keys = (
    exploded
    .select("level", "host")
    .exceptAll(
        df_app_logs
        .filter(F.col("service") == "auth")
        .select("level", "host")
    )
)

verified = (
    exploded
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 16:00:00")))
    .select("*", F.trim("level").alias("clean_level"))
)

valid = (
    filtered
    .filter(F.col("key").isNotNull() & (F.col("log_ts") >= F.lit("2025-06-01 13:00:00").cast("timestamp")))
    .withColumn("level_upper", F.upper("level"))
    .withColumn("clean_level_2", F.trim("level"))
)

cleaned = (
    valid
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "host",
        "log_ts",
        "value",
        "level_upper",
        "clean_level_2",
        F.when(F.col("key") == "version", 1).otherwise(0).alias("is_version"),
    )
)

result = (
    cleaned
    .groupBy("host")
    .agg(
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_count"),
        F.count_if(F.col("host") == "h3").alias("h3_events"),
        F.round(F.avg("is_version"), 2).alias("avg_is_version"),
        F.sum(F.when(F.col("host") == "h2", F.col("is_version")).otherwise(0)).alias("h2_is_version"),
        F.min("log_ts").alias("first_seen"),
        F.size(F.collect_set("host")).alias("num_hosts"),
    )
    .filter(F.col("h3_events") > 1)
    .orderBy(F.col("h2_is_version").asc_nulls_first())
)
