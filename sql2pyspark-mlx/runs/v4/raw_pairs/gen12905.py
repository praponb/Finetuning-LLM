from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        "context",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.col("context")["trace"].alias("trace"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.stddev_samp("status_code_high").over(w).alias("host_stddev_samp_status_code_high"),
    )
)
