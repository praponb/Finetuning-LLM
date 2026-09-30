from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "level").orderBy("status_code", "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

cleaned = (
    lines
    .filter((~F.col("key").eqNullSafe(F.lit("region"))) & (F.col("log_ts") > F.lit("2025-06-01 11:00:00").cast("timestamp")))
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        "key",
        F.col("latency_ms").cast("string").alias("latency_ms_num"),
        F.coalesce("key", F.lit("NONE")).alias("key_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "key", "latency_ms_num")
)

filtered = (
    ranked
    .filter(F.col("key").isin(["version", "region"]))
    .select("*", F.initcap(F.trim("key")).alias("key_title"))
)

result = (
    filtered
    .groupBy("key")
    .agg(
        F.count_if(F.col("key") == "region").alias("region_events"),
        F.count(F.when(F.col("key") == "region", 1)).alias("region_count"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count(F.when(F.col("key") == "region", 1)).alias("region_rows"),
    )
    .filter(F.col("region_count") < 1000)
)
