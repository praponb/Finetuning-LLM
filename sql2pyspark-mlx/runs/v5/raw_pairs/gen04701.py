from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("key", "host")
w5 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

parsed = (
    exploded
    .select(
        "service",
        "host",
        "log_ts",
        "status_code",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

matched_keys = (
    parsed
    .select("host")
    .subtract(
        df_app_logs
        .filter(F.col("level").isin(["ERROR", "INFO", "DEBUG"]))
        .select("host")
    )
)

reconciled = (
    parsed
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "host",
        "log_ts",
        "status_code",
        "key",
        F.count("status_code_high").over(w).alias("host_count_status_code_high"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.avg("host_count_status_code_high").over(w2).alias("host_count_status_code_high_avg_15m"),
    )
)

enriched_app_logs = (
    enriched
    .select(
        "host",
        "log_ts",
        "host_count_status_code_high",
        "status_code_share",
        "host_count_status_code_high_avg_15m",
        F.avg("host_count_status_code_high").over(w3).alias("host_count_status_code_high_avg_1h"),
        F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4).alias("status_code_share_2"),
    )
)

scored = (
    enriched_app_logs
    .select(
        "host",
        "log_ts",
        "status_code_share",
        "host_count_status_code_high_avg_15m",
        "host_count_status_code_high_avg_1h",
        "status_code_share_2",
        F.sum("host_count_status_code_high_avg_15m").over(w5).alias("host_count_status_code_high_avg_15m_sum_1h"),
    )
)

result = (
    scored
    .select(
        "log_ts",
        "status_code_share",
        "host_count_status_code_high_avg_15m",
        "status_code_share_2",
        "host_count_status_code_high_avg_15m_sum_1h",
    )
    .orderBy(F.col("log_ts").desc_nulls_first())
)
