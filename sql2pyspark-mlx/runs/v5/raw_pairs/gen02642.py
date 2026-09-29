from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("status_code_whole", "id", "key")
w2 = Window.partitionBy("key")
w3 = Window.orderBy("status_code_whole", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

cleaned = (
    flattened
    .filter((F.col("log_ts") >= F.lit("2025-06-01 07:00:00").cast("timestamp")) & F.col("level").isin(["WARN", "INFO", "DEBUG"]))
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "key",
        "value",
        F.ceil("status_code").alias("status_code_whole"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "level", "latency_ms", "key", "value", "status_code_whole")
)

base = (
    ranked
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "level",
        "latency_ms",
        "key",
        "status_code_whole",
        F.lower("key").alias("key_lower"),
        F.initcap(F.trim("level")).alias("level_title"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_filled"),
    )
)

scored = (
    base
    .select(
        "*",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
        F.first("key", ignorenulls=True).over(w3).alias("first_known_key"),
    )
)

result = (
    scored
    .groupBy("level")
    .agg(
        F.avg(F.when(F.col("key") == "version", F.col("status_code_whole"))).alias("version_avg_status_code_whole"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").asc_nulls_first())
)
