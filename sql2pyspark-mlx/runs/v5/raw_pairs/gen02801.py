from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("service")
w_recent = Window.partitionBy("host", "service").orderBy("latency_ms")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    flattened
    .select(
        "id",
        "latency_ms",
        "attr_key",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.min("latency_ms").over(w2).alias("service_min_latency_ms"),
        F.dense_rank().over(w_recent).alias("latency_ms_dense_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share_2"),
    )
)
