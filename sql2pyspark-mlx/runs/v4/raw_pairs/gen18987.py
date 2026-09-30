from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("key", "level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy("service").orderBy(F.col("latency_ms").asc_nulls_first())
w6 = Window.partitionBy("key")
w7 = Window.partitionBy("service")
w8 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

enriched = (
    exploded
    .select(
        "*",
        F.sum("status_code").over(w_hist).alias("status_code_sum_30m"),
        F.count("latency_ms").over(w2).alias("latency_ms_count_1h"),
    )
)

filtered = (
    enriched
    .filter((F.col("log_ts") > F.to_timestamp(F.lit("2025-06-02 01:00:00"))) & F.col("key").isNotNull())
    .select("*", F.concat("service", F.lit(", "), "key").alias("service_key"))
)

cleaned = (
    filtered
    .filter(F.col("log_ts") > F.to_date(F.lit("2025-06-01")))
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "key",
        "status_code_sum_30m",
        "latency_ms_count_1h",
        "service_key",
        F.trim("service").alias("clean_service"),
        F.upper("service").alias("service_upper"),
    )
)

enriched_app_logs = (
    cleaned
    .withColumn("latency_ms_count_1h_max_2h", F.max("latency_ms_count_1h").over(w3))
    .withColumn("latency_ms_max_1h", F.max("latency_ms").over(w4))
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w5))
)

metrics = (
    enriched_app_logs
    .select(
        "*",
        F.round((F.col("latency_ms_count_1h_max_2h") / F.sum("latency_ms_count_1h_max_2h").over(w6)), 4).alias("latency_ms_count_1h_max_2h_share"),
        F.round((F.col("latency_ms_count_1h") / F.sum("latency_ms_count_1h").over(w7)), 4).alias("latency_ms_count_1h_share"),
        F.count("status_code_sum_30m").over(w8).alias("status_code_sum_30m_count_2h"),
    )
)

result = (
    metrics
    .select(
        "service",
        "latency_ms_count_1h_share",
        "latency_ms_cume_dist",
        "latency_ms_count_1h_max_2h_share",
    )
    .unpivot(
        ["service"],
        ["latency_ms_count_1h_share", "latency_ms_cume_dist", "latency_ms_count_1h_max_2h_share"],
        "measure",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
