from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("level", "service")
w_hist = Window.orderBy("status_code")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    flattened
    .select(
        "*",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.avg("latency_ms").over(w2).alias("level_avg_latency_ms"),
        F.dense_rank().over(w_hist).alias("status_code_dense_rank"),
        F.rank().over(w_hist).alias("status_code_rank"),
    )
)
