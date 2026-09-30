from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.orderBy(F.col("latency_ms").desc_nulls_last())
w4 = Window.orderBy("level_max_is_info")
w5 = Window.partitionBy("level").orderBy(F.col("is_info_max_15m").desc_nulls_last())
w6 = Window.partitionBy("level").orderBy(F.col("latency_ms_cume_dist_minus_is_info_max_15m").asc_nulls_last())
w7 = Window.orderBy(F.col("is_info_max_15m_dense_rank").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

valid = (
    lines
    .filter((F.col("log_ts") >= F.lit("2025-06-01 15:00:00").cast("timestamp")) & (~F.col("attr_key").eqNullSafe(F.lit("version"))))
    .withColumn("service_group", F.when(F.col("service").isin(["auth", "checkout"]), "Priority").otherwise("Other"))
)

base = (
    valid
    .filter(F.col("level") == "ERROR")
    .withColumn("is_info", F.when(F.col("level") == "INFO", 1).otherwise(0))
    .withColumn("attr_key_upper", F.upper("attr_key"))
)

metrics = (
    base
    .select(
        "id",
        "service",
        "level",
        "v",
        "service_group",
        "attr_key_upper",
        F.max("is_info").over(w).alias("level_max_is_info"),
        F.max("is_info").over(w2).alias("is_info_max_15m"),
        F.cume_dist().over(w3).alias("latency_ms_cume_dist"),
    )
)

valid_app_logs = (
    metrics
    .filter(F.col("level") != "ERROR")
    .select(
        "id",
        "service",
        "level",
        "v",
        "service_group",
        "attr_key_upper",
        "level_max_is_info",
        "is_info_max_15m",
        "latency_ms_cume_dist",
        (F.col("latency_ms_cume_dist") - F.col("is_info_max_15m")).alias("latency_ms_cume_dist_minus_is_info_max_15m"),
        F.greatest("level_max_is_info", "is_info_max_15m").alias("greatest_level_max_is_info_is_info_max_15m"),
    )
)

metrics_app_logs = (
    valid_app_logs
    .withColumn("level_max_is_info_dense_rank", F.dense_rank().over(w4))
    .withColumn("is_info_max_15m_dense_rank", F.dense_rank().over(w5))
)

with_windows = (
    metrics_app_logs
    .withColumn("latency_ms_cume_dist_minus_is_info_max_15m_rank", F.rank().over(w6))
    .withColumn("level_min_is_info_max_15m", F.min("is_info_max_15m").over(w))
    .withColumn("is_info_max_15m_dense_rank_dense_rank", F.dense_rank().over(w7))
)

result = (
    with_windows
    .select(
        "level",
        F.col("greatest_level_max_is_info_is_info_max_15m").alias("GREATEST_LEVEL_MAX_IS_INFO_IS_INFO_MAX_15M_VAL"),
        "latency_ms_cume_dist_minus_is_info_max_15m",
    )
    .unpivot(
        ids=["level"],
        values=["GREATEST_LEVEL_MAX_IS_INFO_IS_INFO_MAX_15M_VAL", "latency_ms_cume_dist_minus_is_info_max_15m"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
