from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "level")
w2 = Window.partitionBy("level", "k")
w3 = Window.partitionBy("k")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

cleaned = (
    exploded
    .filter(~F.col("k").eqNullSafe(F.lit("version")))
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "k",
        "value",
        F.upper("host").alias("host_upper"),
    )
)

matched_keys = (
    cleaned
    .select("id", "level")
    .subtract(
        df_app_logs
        .filter(F.col("status_code") >= 325)
        .select("id", "level")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .filter(F.col("status_code") >= 357)
    .select(
        "*",
        F.avg("status_code").over(w).alias("k_avg_status_code"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share"),
    )
)

grouped = (
    metrics
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"), "level")
    .agg(
        F.size(F.collect_set("k")).alias("num_ks"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    grouped
    .select(
        "hour_window",
        "level",
        "num_ks",
        F.when(F.col("level").rlike("^(INFO|DEBUG)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("level_zone"),
    )
    .orderBy(F.col("hour_window").desc_nulls_last(), F.col("num_ks").desc())
)
