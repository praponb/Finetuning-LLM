from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("host")
w2 = Window.partitionBy("service")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    lines
    .select(
        "*",
        F.corr("latency_ms", "status_code").over(w_hist).alias("corr_latency_ms_status_code"),
        F.stddev_samp("latency_ms").over(w2).alias("service_stddev_samp_latency_ms"),
    )
)
