from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.orderBy("status_code_high")
w3 = Window.partitionBy("attr_key").orderBy(F.col("status_code_high").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

parsed = (
    exploded
    .filter(F.col("status_code") == 293)
    .select(
        "status_code",
        "attr_key",
        "v",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    parsed
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w)), 4))
    .withColumn("status_code_high_dense_rank", F.dense_rank().over(w2))
)

with_windows_app_logs = (
    with_windows
    .select(
        "*",
        F.rank().over(w3).alias("status_code_high_rank"),
        F.sum("flag_bit8").over(w).alias("attr_key_sum_flag_bit8"),
    )
)

result = (
    with_windows_app_logs
    .select(
        "attr_key",
        "flag_bit8",
        "status_code_high",
        "status_code_high_dense_rank",
        "status_code_high_rank",
        "attr_key_sum_flag_bit8",
    )
)
