from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

kept_keys = (
    exploded
    .select("id", "level")
    .subtract(
        df_app_logs
        .filter(~F.col("level").isin(["DEBUG", "WARN"]))
        .select("id", "level")
    )
)

verified = (
    exploded
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter((F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 06:00:00"))) & F.col("k").isNotNull())
    .withColumn("k_length", F.length("k"))
    .withColumn("k_filled", F.coalesce("k", F.lit("UNKNOWN")))
)

result = (
    filtered
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.round(F.sum("k_length") / F.nullif(F.count(F.when(F.col("level") == "INFO", 1)), F.lit(0)), 2).alias("k_length_per_info"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.sum(F.when(F.col("k") == "region", F.col("k_length")).otherwise(0)).alias("region_k_length"),
        F.sum(F.when(F.col("k") == "trace", F.col("k_length")).otherwise(0)).alias("trace_k_length"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.round(F.sum("k_length") / F.nullif(F.count(F.when(F.col("level") == "DEBUG", 1)), F.lit(0)), 2).alias("k_length_per_debug"),
    )
    .filter(F.col("k_length_per_debug") >= 0)
    .orderBy(F.col("num_levels").asc_nulls_last())
)
