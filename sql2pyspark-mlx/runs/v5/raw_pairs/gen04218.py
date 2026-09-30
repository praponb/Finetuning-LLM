from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w_running = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("service", "host")
w4 = Window.partitionBy("attr_key").orderBy(F.col("latency_ms_max_1h").desc_nulls_last())
w_hist = Window.partitionBy("attr_key").orderBy(F.col("service_max_latency_ms").desc_nulls_last())
w_grp = Window.partitionBy("attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

scored = (
    exploded
    .select(
        "id",
        "status_code",
        "attr_key",
        "v",
        F.sum("latency_ms").over(w).alias("host_sum_latency_ms"),
        F.max("latency_ms").over(w_running).alias("latency_ms_max_1h"),
        F.max("latency_ms").over(w3).alias("service_max_latency_ms"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.rank().over(w4).alias("latency_ms_max_1h_rank"),
        F.rank().over(w_hist).alias("service_max_latency_ms_rank"),
    )
)

scored_app_logs = (
    enriched
    .filter(F.col("attr_key") == "region")
    .withColumn("attr_key_avg_host_sum_latency_ms", F.avg("host_sum_latency_ms").over(w_grp))
)

result = (
    scored_app_logs
    .select(
        "id",
        "status_code",
        "attr_key",
        "service_max_latency_ms",
        "service_max_latency_ms_rank",
        "attr_key_avg_host_sum_latency_ms",
        F.when(F.col("attr_key").rlike("^(region|trace)$"), "GROUP_A").otherwise("PRIORITY").alias("attr_key_zone"),
    )
    .orderBy(F.col("status_code").desc_nulls_first(), "attr_key_zone")
)
