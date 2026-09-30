from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("k")
w3 = Window.partitionBy("k").orderBy(F.col("latency_ms_max_2h").desc())
w4 = Window.partitionBy("service").orderBy(F.col("k_min_latency_ms").asc_nulls_first())
w_recent = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.partitionBy("service").orderBy(F.col("latency_ms_sum_30m").desc())
w_running = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w8 = Window.partitionBy("service")
w9 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "v"),
    )
)

scored = (
    exploded
    .filter(F.col("latency_ms").between(1101, 2245))
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "k",
        F.max("latency_ms").over(w).alias("latency_ms_max_2h"),
    )
)

base = (
    scored
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "k",
        "latency_ms_max_2h",
        F.when(F.col("k").isin(["region", "trace"]), "Priority").otherwise("Other").alias("k_group"),
    )
)

with_windows = (
    base
    .select(
        "service",
        "log_ts",
        "latency_ms",
        "latency_ms_max_2h",
        "k_group",
        F.min("latency_ms").over(w2).alias("k_min_latency_ms"),
        F.percent_rank().over(w3).alias("latency_ms_max_2h_percent_rank"),
    )
)

scored_app_logs = (
    with_windows
    .withColumn("k_min_latency_ms_rank", F.rank().over(w4))
    .withColumn("latency_ms_sum_30m", F.sum("latency_ms").over(w_recent))
)

scored_app_logs_2 = (
    scored_app_logs
    .select(
        "*",
        F.percent_rank().over(w6).alias("latency_ms_sum_30m_percent_rank"),
        F.count("latency_ms_max_2h").over(w_running).alias("latency_ms_max_2h_count_15m"),
        F.max("latency_ms").over(w8).alias("service_max_latency_ms"),
        F.avg("k_min_latency_ms_rank").over(w9).alias("k_min_latency_ms_rank_avg_1h"),
        F.round((F.col("latency_ms_max_2h") / F.sum("latency_ms_max_2h").over(w8)), 4).alias("latency_ms_max_2h_share"),
    )
)

summary = (
    scored_app_logs_2
    .groupBy(
        F.when(F.col("service_max_latency_ms") >= 2456, "LARGE").otherwise("SMALL").alias("service_max_latency_ms_tier"),
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
    )
    .agg(
        F.sum(F.when(F.col("service") == "billing", F.col("latency_ms_sum_30m_percent_rank")).otherwise(0)).alias("billing_latency_ms_sum_30m_percent_rank"),
        F.avg(F.when(F.col("service") == "search", F.col("latency_ms_max_2h_percent_rank"))).alias("search_avg_latency_ms_max_2h_percent_rank"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.size(F.collect_set("service")).alias("num_services"),
    )
)

result = (
    summary
    .select(
        "service_max_latency_ms_tier",
        "activity_day",
        "billing_latency_ms_sum_30m_percent_rank",
        "search_avg_latency_ms_max_2h_percent_rank",
        "service_list",
        "num_services",
    )
    .orderBy(F.col("service_max_latency_ms_tier").desc(), "activity_day")
)
