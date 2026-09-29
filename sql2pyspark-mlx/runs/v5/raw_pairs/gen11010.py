from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("service").orderBy(F.col("latency_ms").desc())
w3 = Window.partitionBy("service")
w4 = Window.partitionBy("level").orderBy(F.col("latency_ms_num").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

cleaned = (
    exploded
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "v",
        F.col("latency_ms").cast("double").alias("latency_ms_num"),
        F.initcap(F.trim("key")).alias("key_title"),
    )
)

scored = (
    cleaned
    .filter(F.col("level") == "WARN")
    .select(
        "level",
        "latency_ms",
        "status_code",
        "v",
        "latency_ms_num",
        "key_title",
        F.max("latency_ms_num").over(w).alias("latency_ms_num_max_15m"),
        F.dense_rank().over(w2).alias("latency_ms_dense_rank"),
        F.sum("status_code").over(w3).alias("service_sum_status_code"),
        F.cume_dist().over(w4).alias("latency_ms_num_cume_dist"),
        F.sum("status_code").over(w).alias("status_code_sum_15m"),
    )
)

matched_keys = (
    scored
    .select("level")
    .subtract(
        df_app_logs
        .filter(F.col("latency_ms") <= 2205)
        .select("level")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("status_code") > 437)
    .select(
        "*",
        F.round("latency_ms_num_max_15m", 2).alias("latency_ms_num_max_15m_rounded"),
    )
)

result = (
    base
    .groupBy(
        "level",
        F.when(F.col("service_sum_status_code") >= 1756, "PREMIUM").otherwise("STANDARD").alias("service_sum_status_code_tier"),
        F.when(F.col("status_code") >= 454, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.avg("status_code_sum_15m").alias("avg_status_code_sum_15m"),
        F.count(F.when(F.col("level") == "INFO", 1)).alias("info_rows"),
    )
)
