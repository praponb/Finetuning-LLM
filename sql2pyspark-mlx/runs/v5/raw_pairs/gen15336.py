from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("status_code_high_filled").desc_nulls_last())
w2 = Window.partitionBy("key").orderBy(F.col("least_status_code_high_latency_ms").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

parsed = (
    flattened
    .select(
        "latency_ms",
        "status_code",
        "key",
        "attr_value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

base = (
    parsed
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.least("status_code_high", "latency_ms").alias("least_status_code_high_latency_ms"),
    )
)

filtered = (
    base
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("region"))))
    .select(
        "latency_ms",
        "status_code",
        "key",
        "attr_value",
        "status_code_high",
        "least_status_code_high_latency_ms",
        F.coalesce("status_code_high", F.lit(0)).alias("status_code_high_filled"),
        F.coalesce("key", F.lit("unassigned")).alias("key_filled"),
    )
)

with_windows = (
    filtered
    .select(
        "latency_ms",
        "key",
        "attr_value",
        "status_code_high_filled",
        "key_filled",
        F.rank().over(w).alias("status_code_high_filled_rank"),
        F.cume_dist().over(w2).alias("least_status_code_high_latency_ms_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "latency_ms",
        "attr_value",
        "status_code_high_filled",
        "key_filled",
        "status_code_high_filled_rank",
        "least_status_code_high_latency_ms_cume_dist",
    )
    .orderBy("status_code_high_filled_rank")
)
