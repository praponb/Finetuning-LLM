from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("service")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "v"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "level",
        "k",
        F.stddev_samp("latency_ms").over(w_recent).alias("service_stddev_samp_latency_ms"),
        F.min("latency_ms").over(w_recent).alias("service_min_latency_ms"),
    )
)
