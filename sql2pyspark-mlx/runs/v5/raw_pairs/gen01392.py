from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc())
w2 = Window.orderBy(F.col("is_flagged").asc_nulls_last())
w3 = Window.orderBy(F.col("latency_ms").desc_nulls_last())
w4 = Window.orderBy(F.col("status_code_dense_rank").asc_nulls_first())
w_running = Window.orderBy(F.col("status_code").desc())
w6 = Window.orderBy(F.col("is_flagged_dense_rank").asc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

flattened_attrs = (
    lines
    .select(
        "latency_ms",
        "status_code",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
    )
)

with_windows = (
    flattened_attrs
    .select(
        "latency_ms",
        "status_code",
        F.dense_rank().over(w).alias("status_code_dense_rank"),
        F.dense_rank().over(w2).alias("is_flagged_dense_rank"),
    )
)

filtered = (
    with_windows
    .filter(F.col("status_code_dense_rank") >= 6)
    .withColumn("status_code_times_status_code_dense_rank", (F.col("status_code") * F.col("status_code_dense_rank")))
    .withColumn("latency_ms_adjusted", F.round((F.col("latency_ms") * 0.85), 2))
)

with_windows_app_logs = (
    filtered
    .filter(F.col("latency_ms") == 1419)
    .withColumn("latency_ms_rank", F.rank().over(w3))
    .withColumn("status_code_dense_rank_rank", F.rank().over(w4))
    .withColumn("status_code_dense_rank_2", F.dense_rank().over(w_running))
    .withColumn("is_flagged_dense_rank_rank", F.rank().over(w6))
)

result = (
    with_windows_app_logs
    .select(
        "latency_ms",
        "status_code",
        "is_flagged_dense_rank",
        "status_code_times_status_code_dense_rank",
        "latency_ms_adjusted",
        "status_code_dense_rank_rank",
        "status_code_dense_rank_2",
        "is_flagged_dense_rank_rank",
    )
)
