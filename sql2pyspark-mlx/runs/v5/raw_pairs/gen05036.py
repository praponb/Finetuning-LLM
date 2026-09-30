from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_grp = Window.partitionBy("service", "level")
w3 = Window.partitionBy("service")
w4 = Window.orderBy(F.col("status_code_avg_30m").asc_nulls_last())
w5 = Window.partitionBy("level").orderBy(F.col("status_code_max_30m").asc_nulls_first())
w6 = Window.partitionBy("level", "service")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

enriched = (
    lines
    .filter(F.col("k").isin(["version", "region", "trace"]))
    .select(
        "service",
        "level",
        F.avg("status_code").over(w_part).alias("status_code_avg_30m"),
        F.max("status_code").over(w_part).alias("status_code_max_30m"),
        F.stddev_samp("status_code").over(w_grp).alias("service_stddev_samp_status_code"),
    )
)

metrics = (
    enriched
    .withColumn("status_code_avg_30m_share", F.round((F.col("status_code_avg_30m") / F.sum("status_code_avg_30m").over(w3)), 4))
)

enriched_app_logs = (
    metrics
    .select(
        "*",
        F.dense_rank().over(w4).alias("status_code_avg_30m_dense_rank"),
        F.dense_rank().over(w5).alias("status_code_max_30m_dense_rank"),
        F.round((F.col("service_stddev_samp_status_code") / F.sum("service_stddev_samp_status_code").over(w6)), 4).alias("service_stddev_samp_status_code_share"),
    )
)

stats = (
    enriched_app_logs
    .rollup("level")
    .agg(
        F.sum(F.when(F.col("level") == "DEBUG", F.col("status_code_max_30m")).otherwise(0)).alias("debug_status_code_max_30m"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count"),
        F.grouping("level").alias("grouping_id"),
    )
    .filter(F.col("debug_status_code_max_30m") < 1000000)
)

result = stats.select("debug_status_code_max_30m", "num_services", "grouping_id")
