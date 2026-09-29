from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").desc_nulls_first())
w2 = Window.partitionBy("host", "attr_key")
w3 = Window.partitionBy("service", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    lines
    .select(
        "*",
        F.percent_rank().over(w).alias("latency_ms_percent_rank"),
        F.corr("status_code", "latency_ms").over(w2).alias("corr_status_code_latency_ms"),
        F.corr("latency_ms", "status_code").over(w3).alias("corr_latency_ms_status_code"),
    )
)
