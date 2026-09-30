from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("level")
w5 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w6 = Window.partitionBy("level", "k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w7 = Window.partitionBy("host")
w8 = Window.partitionBy("k").orderBy(F.col("status_code_max_1h").desc_nulls_first())
w9 = Window.partitionBy("level_stddev_samp_status_code_avg_15m_tier").orderBy(F.col("num_hosts").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

with_windows = (
    exploded
    .select(
        "*",
        F.max("status_code").over(w).alias("status_code_max_1h"),
        F.sum("status_code").over(w2).alias("status_code_sum_15m"),
        F.count("status_code").over(w3).alias("status_code_count_1h"),
    )
)

metrics = (
    with_windows
    .withColumn("level_stddev_samp_status_code", F.stddev_samp("status_code").over(w4))
    .withColumn("status_code_max_1h_sum_2h", F.sum("status_code_max_1h").over(w5))
)

enriched = (
    metrics
    .withColumn("level_stddev_samp_status_code_avg_15m", F.avg("level_stddev_samp_status_code").over(w6))
    .withColumn("host_stddev_samp_status_code_count_1h", F.stddev_samp("status_code_count_1h").over(w7))
    .withColumn("status_code_max_1h_dense_rank", F.dense_rank().over(w8))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("level_stddev_samp_status_code_avg_15m") >= 469, "LARGE").otherwise("SMALL").alias("level_stddev_samp_status_code_avg_15m_tier"),
        "level",
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
    )
    .agg(
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.count(F.when(F.col("k") == "version", 1)).alias("version_rows"),
        F.count(F.when(F.col("k") == "region", 1)).alias("region_rows"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.count_if(F.col("host") == "h1").alias("h1_events"),
        F.max("log_ts").alias("ended_at"),
    )
    .withColumn("num_hosts_percent_rank", F.percent_rank().over(w9))
)
