from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w2 = Window.partitionBy("key").orderBy(F.col("least_latency_ms_status_code").desc_nulls_last())
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("key").orderBy(F.col("adjusted_status_code").asc_nulls_last())
w5 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

filtered = (
    flattened
    .filter(F.col("key").isNotNull() & (F.col("latency_ms") == 1739))
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "key",
        "attr_value",
        F.least("latency_ms", "status_code").alias("least_latency_ms_status_code"),
    )
)

base = (
    filtered
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("key").isNotNull())
    .withColumn("key_group", F.when(F.col("key").isin(["version", "trace"]), "Priority").otherwise("Other"))
    .withColumn("resolved_key", F.coalesce("key", F.lit("NONE")))
)

base_app_logs = (
    base
    .filter(F.col("key").isNotNull())
    .withColumn("key_upper", F.upper("key"))
    .withColumn("log_ts_date", F.to_date("log_ts"))
)

base_app_logs_2 = (
    base_app_logs
    .filter(F.col("log_ts") > F.lit("2025-06-01").cast("date"))
    .select(
        "*",
        F.round((F.col("status_code") * 1.1), 2).alias("adjusted_status_code"),
        F.to_date("log_ts").alias("log_ts_date_2"),
    )
)

with_windows = (
    base_app_logs_2
    .withColumn("key_min_latency_ms", F.min("latency_ms").over(w))
    .withColumn("least_latency_ms_status_code_cume_dist", F.cume_dist().over(w2))
    .withColumn("key_count_least_latency_ms_status_code", F.count("least_latency_ms_status_code").over(w))
)

enriched = (
    with_windows
    .filter(F.col("log_ts") < "2023-11-01")
    .withColumn("least_latency_ms_status_code_cume_dist_avg_1h", F.avg("least_latency_ms_status_code_cume_dist").over(w3))
    .withColumn("adjusted_status_code_cume_dist", F.cume_dist().over(w4))
    .withColumn("key_count_least_latency_ms_status_code_max_30m", F.max("key_count_least_latency_ms_status_code").over(w5))
    .withColumn("least_latency_ms_status_code_rank", F.rank().over(w2))
    .withColumn("corr_status_code_latency_ms", F.corr("status_code", "latency_ms").over(w))
)

result = (
    enriched
    .groupBy("key", F.date_trunc("month", "log_ts_date_2").alias("activity_month"))
    .agg(
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.percentile_approx("least_latency_ms_status_code_rank", 0.5).alias("least_latency_ms_status_code_rank_p50"),
        F.sum(F.when(F.col("key") == "version", F.col("least_latency_ms_status_code_rank")).otherwise(0)).alias("version_least_latency_ms_status_code_rank"),
        F.max("log_ts").alias("last_seen"),
        F.min("log_ts").alias("first_seen"),
    )
    .orderBy(F.col("key").asc_nulls_last())
)
