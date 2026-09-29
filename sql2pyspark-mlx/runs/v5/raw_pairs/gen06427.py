from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("key")
w2 = Window.partitionBy("key").orderBy(F.col("has_fault_plus_status_code_whole").asc_nulls_first())
w_grp = Window.orderBy(F.col("has_fault_plus_status_code_whole").desc_nulls_first())
w_recent = Window.orderBy(F.col("key_sum_latency_ms").desc_nulls_last())
w5 = Window.orderBy("has_fault_plus_status_code_whole_dense_rank_2")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

unpacked = (
    flattened
    .filter(F.col("key") == "trace")
    .select(
        "level",
        "latency_ms",
        "status_code",
        "key",
        "value",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

filtered = (
    unpacked
    .filter(F.col("status_code_high").isNotNull())
    .select(
        "level",
        "latency_ms",
        "status_code",
        "key",
        "value",
        "has_fault",
        F.ceil("status_code").alias("status_code_whole"),
    )
)

base = (
    filtered
    .filter(F.col("latency_ms") < 1234)
    .select(
        "latency_ms",
        "key",
        "value",
        "has_fault",
        "status_code_whole",
        (F.col("has_fault") + F.col("status_code_whole")).alias("has_fault_plus_status_code_whole"),
    )
)

with_windows = (
    base
    .select(
        "has_fault",
        "has_fault_plus_status_code_whole",
        F.avg("has_fault_plus_status_code_whole").over(w_hist).alias("key_avg_has_fault_plus_status_code_whole"),
        F.sum("latency_ms").over(w_hist).alias("key_sum_latency_ms"),
        F.dense_rank().over(w2).alias("has_fault_plus_status_code_whole_dense_rank"),
    )
)

scored = (
    with_windows
    .withColumn("has_fault_plus_status_code_whole_dense_rank_2", F.dense_rank().over(w_grp))
    .withColumn("key_sum_latency_ms_dense_rank", F.dense_rank().over(w_recent))
)

filtered_app_logs = (
    scored
    .filter(F.col("has_fault_plus_status_code_whole_dense_rank_2").between(3, 4))
    .select(
        "*",
        F.coalesce("key_sum_latency_ms_dense_rank", F.lit(0)).alias("key_sum_latency_ms_dense_rank_filled"),
    )
)

result = (
    filtered_app_logs
    .select(
        "has_fault_plus_status_code_whole_dense_rank",
        "has_fault_plus_status_code_whole_dense_rank_2",
        "key_sum_latency_ms_dense_rank_filled",
        F.percent_rank().over(w5).alias("has_fault_plus_status_code_whole_dense_rank_2_percent_rank"),
    )
)
