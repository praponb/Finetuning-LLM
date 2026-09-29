from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("flag_bit4").asc_nulls_last())
w2 = Window.orderBy(F.col("flag_bit4").desc())
w_part = Window.partitionBy("attr_key")
w_hist = Window.partitionBy("attr_key").orderBy(F.col("flag_bit4").desc_nulls_last())
w_ordered = Window.partitionBy("service").orderBy(F.col("latency_ms_plus_status_code_high").asc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    lines
    .select(
        "service",
        "latency_ms",
        "status_code",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
    )
)

base = (
    parsed
    .filter(F.col("attr_key").isNotNull() & F.col("status_code").between(334, 404))
    .select(
        "service",
        "latency_ms",
        "status_code",
        "attr_key",
        "flag_bit4",
        (F.col("latency_ms") + F.col("status_code_high")).alias("latency_ms_plus_status_code_high"),
    )
)

metrics = (
    base
    .select(
        "service",
        "latency_ms",
        "flag_bit4",
        "latency_ms_plus_status_code_high",
        F.rank().over(w).alias("flag_bit4_rank"),
        F.dense_rank().over(w2).alias("flag_bit4_dense_rank"),
        F.sum("latency_ms").over(w_part).alias("attr_key_sum_latency_ms"),
        F.dense_rank().over(w_hist).alias("flag_bit4_dense_rank_2"),
        F.percent_rank().over(w_ordered).alias("latency_ms_plus_status_code_high_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "service",
        "flag_bit4",
        "latency_ms_plus_status_code_high",
        "flag_bit4_rank",
        "flag_bit4_dense_rank_2",
    )
    .orderBy(F.col("flag_bit4_rank").asc_nulls_last())
)
