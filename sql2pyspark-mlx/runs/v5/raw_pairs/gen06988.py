from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("latency_ms").desc())
w2 = Window.partitionBy("k").orderBy("latency_ms")
w3 = Window.partitionBy("level")
w4 = Window.partitionBy("level").orderBy(F.col("latency_ms").desc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "v"),
    )
)

with_windows = (
    lines
    .filter(F.col("service").isin(["checkout", "auth"]))
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.percent_rank().over(w).alias("latency_ms_percent_rank"),
        F.rank().over(w2).alias("latency_ms_rank"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "latency_ms_rank",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share"),
        F.dense_rank().over(w4).alias("latency_ms_dense_rank"),
        F.stddev_samp("latency_ms").over(w3).alias("level_stddev_samp_latency_ms"),
    )
)

remaining_keys = (
    scored
    .select("id", "host")
    .subtract(
        df_app_logs
        .filter(F.col("host").isin(["h1", "h2", "h4"]))
        .select("id", "host")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("level_stddev_samp_latency_ms") != 702)
    .select(
        "*",
        F.when(F.col("host").isin(["h3", "h2"]), "Core").otherwise("Other").alias("host_group"),
    )
)

matched_keys = (
    valid
    .select("host", "id")
    .intersectAll(
        df_app_logs
        .filter(~F.col("host").isin(["h4", "h1", "h3"]))
        .select("host", "id")
    )
)

reconciled_app_logs = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "latency_ms_dense_rank",
        "level_stddev_samp_latency_ms",
        F.when(F.col("level").rlike("^(WARN|DEBUG)$"), "PRIORITY").otherwise("CORE").alias("level_zone"),
    )
    .orderBy(F.col("latency_ms_dense_rank").desc_nulls_last(), "level_zone")
)
