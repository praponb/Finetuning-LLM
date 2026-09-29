from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key")
w2 = Window.partitionBy("attr_key")
w_hist = Window.partitionBy("service", "level").orderBy(F.col("latency_ms").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    lines
    .filter(F.col("latency_ms") > 1231)
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "attr_key",
        "v",
        F.row_number().over(w).alias("row_seq"),
        F.avg("latency_ms").over(w2).alias("attr_key_avg_latency_ms"),
        F.rank().over(w_hist).alias("latency_ms_rank"),
    )
)
