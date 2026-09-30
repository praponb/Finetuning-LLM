from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w_running = Window.partitionBy("level").orderBy("status_code")
w3 = Window.partitionBy("level").orderBy(F.col("latency_ms").desc())
w4 = Window.partitionBy("level").orderBy(F.col("flag_bit2").asc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

parsed = (
    exploded
    .filter(F.col("status_code") == 368)
    .select(
        "level",
        "latency_ms",
        "status_code",
        "key",
        "attr_value",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    parsed
    .filter(F.col("key").isNotNull() & F.col("key").isin(["region", "version"]))
    .select(
        "*",
        (F.col("latency_ms") - F.col("flag_bit2")).alias("latency_ms_minus_flag_bit2"),
    )
)

with_windows = (
    valid
    .filter(F.col("key") == "region")
    .select(
        "*",
        F.stddev_samp("latency_ms").over(w).alias("level_stddev_samp_latency_ms"),
        F.dense_rank().over(w_running).alias("status_code_dense_rank"),
        F.dense_rank().over(w3).alias("latency_ms_dense_rank"),
        F.rank().over(w4).alias("flag_bit2_rank"),
    )
)

result = (
    with_windows
    .select(
        "level",
        "latency_ms",
        "key",
        "attr_value",
        "flag_bit2",
        "status_code_high",
        "latency_ms_minus_flag_bit2",
        "level_stddev_samp_latency_ms",
        "latency_ms_dense_rank",
    )
    .orderBy(F.col("level").asc_nulls_first())
)
