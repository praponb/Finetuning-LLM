from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

gaps = (
    exploded
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

kept_keys = (
    islands
    .select("level", "id")
    .subtract(
        df_app_logs
        .filter(F.col("log_ts") >= "2023-06-01")
        .select("level", "id")
    )
)

reconciled = (
    islands
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("attr_key").isNotNull())
    .select(
        "host",
        "level",
        "attr_key",
        "attr_value",
        "session_start_flag",
        "island_id",
        (F.col("status_code") - F.col("session_start_flag")).alias("status_code_minus_session_start_flag"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_filled"),
    )
)

cleaned = (
    valid
    .filter(F.col("session_start_flag") == 1)
    .withColumn("status_code_minus_session_start_flag_level", F.when(F.col("status_code_minus_session_start_flag") >= 89, "High").when(F.col("status_code_minus_session_start_flag") >= 18, "Medium").otherwise("Low"))
)

valid_app_logs = (
    cleaned
    .filter(F.col("session_start_flag") <= 0)
    .select(
        "host",
        "level",
        "attr_key",
        "attr_value",
        "island_id",
        "status_code_minus_session_start_flag",
        "status_code_minus_session_start_flag_level",
        F.concat("attr_key", F.lit(" - "), "host").alias("attr_key_host"),
    )
)

stats = (
    valid_app_logs
    .rollup(
        "host",
        F.when(F.col("island_id") >= 6, "HIGH").otherwise("LOW").alias("island_id_tier"),
    )
    .agg(
        F.count(F.when(F.col("host") == "h4", 1)).alias("h4_count"),
        F.max("island_id").alias("max_island_id"),
        F.round(F.avg("status_code_minus_session_start_flag"), 2).alias("avg_status_code_minus_session_start_flag"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
    )
    .filter(F.col("h4_count") < 1)
)

result = (
    stats
    .select(
        "host",
        "island_id_tier",
        "max_island_id",
        "avg_status_code_minus_session_start_flag",
        "attr_key_list",
    )
    .orderBy(F.col("max_island_id").desc_nulls_first(), F.col("island_id_tier").desc())
)
