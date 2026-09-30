from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w2 = Window.partitionBy("level", "key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("key").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("host").orderBy(F.col("days_since_log_ts_avg_15m").asc_nulls_last(), "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "v"),
    )
)

valid = (
    flattened
    .filter(F.col("log_ts") >= F.lit("2025-06-01 07:00:00").cast("timestamp"))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "key",
        "v",
        F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"),
    )
)

with_windows = (
    valid
    .filter(F.col("latency_ms").between(1079, 1531))
    .select(
        "id",
        "service",
        "host",
        "level",
        "key",
        "v",
        F.min("days_since_log_ts").over(w).alias("key_min_days_since_log_ts"),
        F.avg("days_since_log_ts").over(w2).alias("days_since_log_ts_avg_15m"),
        F.last("key", ignorenulls=True).over(w3).alias("last_known_key"),
    )
)

base = (
    with_windows
    .filter(F.col("service") == "search")
    .withColumn("host_group", F.when(F.col("host").isin(["h1", "h4"]), "Core").otherwise("Other"))
    .withColumn("resolved_key", F.coalesce("key", F.lit("UNKNOWN")))
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "service",
        "host",
        "key",
        "v",
        "days_since_log_ts_avg_15m",
        "last_known_key",
        "host_group",
        "rank_in_group",
        F.when(F.col("host").rlike("^(h2|h4)$"), "CORE").when(F.col("host").rlike("^(h1|h3)$"), "GROUP_A").otherwise("GROUP_B").alias("host_zone"),
    )
)
