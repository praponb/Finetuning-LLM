from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "service")
w_running = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.orderBy(F.col("status_code").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    flattened
    .select(
        "service",
        "latency_ms",
        "v",
        F.corr("latency_ms", "status_code").over(w).alias("corr_latency_ms_status_code"),
        F.count("status_code").over(w_running).alias("status_code_count_30m"),
        F.percent_rank().over(w3).alias("status_code_percent_rank"),
        F.max("status_code").over(w_running).alias("status_code_max_30m"),
    )
)
