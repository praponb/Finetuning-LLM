from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("service").orderBy(F.col("latency_ms").asc_nulls_last())

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
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

parsed = (
    flattened
    .filter(F.col("latency_ms").between(987, 1742))
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    parsed
    .filter(F.col("latency_ms") != 610)
    .select(
        "id",
        "service",
        "latency_ms",
        "is_flagged",
        "status_code_high",
        F.round((F.col("latency_ms") * 1.15), 2).alias("latency_ms_scaled"),
        F.lower("service").alias("service_lower"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.max("is_flagged").over(w).alias("service_max_is_flagged"),
        F.count("is_flagged").over(w).alias("service_count_is_flagged"),
        F.percent_rank().over(w2).alias("latency_ms_percent_rank"),
    )
)

valid_app_logs = (
    enriched
    .filter(F.col("latency_ms") <= 1637)
    .withColumn("service_upper", F.upper("service"))
    .withColumn("latency_ms_percent_rank_per_service_max_is_flagged", (F.col("latency_ms_percent_rank") / F.nullif("service_max_is_flagged", F.lit(0))))
)

valid_app_logs_2 = (
    valid_app_logs
    .filter(F.col("service_max_is_flagged") <= 0)
    .select(
        "service",
        "latency_ms",
        "status_code_high",
        "latency_ms_scaled",
        "service_lower",
        "service_max_is_flagged",
        "service_count_is_flagged",
        "latency_ms_percent_rank_per_service_max_is_flagged",
        (F.col("latency_ms_scaled") * F.col("latency_ms_percent_rank_per_service_max_is_flagged")).alias("latency_ms_scaled_times_latency_ms_percent_rank_per_service_max_is_flagged"),
    )
)

result = (
    valid_app_logs_2
    .select(
        "service",
        "latency_ms",
        "latency_ms_scaled",
        "service_lower",
        "service_max_is_flagged",
        "service_count_is_flagged",
        "latency_ms_percent_rank_per_service_max_is_flagged",
    )
)
