from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.orderBy(F.col("latency_ms").asc_nulls_first())
w3 = Window.orderBy(F.col("status_code").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

metrics = (
    lines
    .select(
        "id",
        "host",
        F.stddev_samp("status_code").over(w).alias("host_stddev_samp_status_code"),
        F.dense_rank().over(w2).alias("latency_ms_dense_rank"),
        F.cume_dist().over(w3).alias("status_code_cume_dist"),
        F.corr("latency_ms", "status_code").over(w).alias("corr_latency_ms_status_code"),
        F.percent_rank().over(w3).alias("status_code_percent_rank"),
    )
)

filtered = (
    metrics
    .filter(F.col("host_stddev_samp_status_code") <= 405)
    .withColumn("abs_corr_latency_ms_status_code", F.abs("corr_latency_ms_status_code"))
)

cleaned = (
    filtered
    .filter(F.col("host").isin(["h4", "h3", "h2"]))
    .select("*", F.lower("host").alias("host_lower"))
)

result = (
    cleaned
    .select(
        "host",
        "status_code_cume_dist",
        "corr_latency_ms_status_code",
        "abs_corr_latency_ms_status_code",
        "host_lower",
        F.when(F.col("host").rlike("^(h2|h1)$"), "PRIORITY").when(F.col("host").rlike("^(h3|h4)$"), "EXTENDED").otherwise("GROUP_A").alias("host_zone"),
    )
    .orderBy(F.col("host").desc_nulls_last(), "status_code_cume_dist")
)
