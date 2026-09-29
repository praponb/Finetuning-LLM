from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w_recent = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_grp = Window.orderBy(F.col("latency_ms").desc_nulls_last())
w_grp_2 = Window.partitionBy("key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

with_windows = (
    lines
    .select(
        "host",
        "log_ts",
        "latency_ms",
        "key",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
    )
)

matched_keys = (
    with_windows
    .select("host")
    .subtract(
        df_app_logs
        .filter(F.col("latency_ms").between(1531, 2259))
        .select("host")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & (F.col("key") == "version"))
    .withColumn("key_filled", F.coalesce("key", F.lit("unassigned")))
    .withColumn("latency_ms_level", F.when(F.col("latency_ms") >= 2447, "High").when(F.col("latency_ms") >= 1146, "Medium").otherwise("Low"))
    .withColumn("key_clean", F.coalesce("key", F.lit("NONE")))
)

scored = (
    filtered
    .select(
        "latency_ms",
        "key",
        "latency_ms_level",
        "key_clean",
        F.sum("latency_ms").over(w).alias("host_sum_latency_ms"),
        F.sum("latency_ms_share").over(w_recent).alias("latency_ms_share_sum_2h"),
        F.percent_rank().over(w_grp).alias("latency_ms_percent_rank"),
        F.count("latency_ms").over(w_grp_2).alias("key_count_latency_ms"),
    )
)

result = (
    scored
    .select(
        "latency_ms",
        "key",
        "key_clean",
        "host_sum_latency_ms",
        "latency_ms_share_sum_2h",
        "latency_ms_percent_rank",
        "key_count_latency_ms",
    )
    .orderBy(F.col("key").asc_nulls_last())
)
