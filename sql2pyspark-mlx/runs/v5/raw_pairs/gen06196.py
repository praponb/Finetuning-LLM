from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy(F.col("status_code").desc())
w_ordered = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("host")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

enriched = (
    flattened
    .select(
        "host",
        "level",
        "latency_ms",
        "status_code",
        "key",
        F.dense_rank().over(w_hist).alias("status_code_dense_rank"),
        F.sum("latency_ms").over(w_ordered).alias("latency_ms_sum_1h"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share"),
    )
)

filtered = (
    with_windows
    .filter(F.col("latency_ms_sum_1h") == 11465)
    .select(
        "host",
        "latency_ms",
        "status_code",
        "key",
        "status_code_dense_rank",
        "latency_ms_sum_1h",
        F.round((F.col("status_code") * 100), 2).alias("status_code_scaled"),
    )
)

result = (
    filtered
    .select("host", "latency_ms", "key", "status_code_dense_rank", "status_code_scaled")
    .orderBy(F.col("status_code_dense_rank").desc())
)
