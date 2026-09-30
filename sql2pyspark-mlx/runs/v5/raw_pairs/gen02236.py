from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("host")
w2 = Window.partitionBy("host").orderBy(F.col("status_code_high").desc())
w3 = Window.partitionBy("host").orderBy(F.col("has_fault").desc_nulls_last())
w4 = Window.orderBy(F.col("status_code_high_scaled").asc_nulls_last())
w5 = Window.partitionBy("host").orderBy(F.col("status_code_high_scaled").asc_nulls_last())
w6 = Window.orderBy(F.col("status_code_high_percent_rank").desc_nulls_last())
w7 = Window.orderBy(F.col("latency_ms").desc())
w8 = Window.orderBy(F.col("status_code_high_scaled_dense_rank").asc_nulls_last())

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

flattened_attrs = (
    lines
    .select(
        "host",
        "latency_ms",
        "status_code",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

filtered = (
    flattened_attrs
    .filter(F.col("has_fault") >= 1)
    .select(
        "*",
        F.round((F.col("status_code_high") * 1.1), 2).alias("status_code_high_scaled"),
    )
)

metrics = (
    filtered
    .withColumn("host_avg_status_code_high", F.avg("status_code_high").over(w_part))
)

enriched = (
    metrics
    .filter(F.col("status_code").between(345, 386))
    .select(
        "latency_ms",
        "attr_value",
        "status_code_high",
        "status_code_high_scaled",
        F.percent_rank().over(w2).alias("status_code_high_percent_rank"),
        F.dense_rank().over(w3).alias("has_fault_dense_rank"),
        F.rank().over(w4).alias("status_code_high_scaled_rank"),
        F.dense_rank().over(w3).alias("has_fault_dense_rank_2"),
        F.dense_rank().over(w5).alias("status_code_high_scaled_dense_rank"),
    )
)

metrics_app_logs = (
    enriched
    .withColumn("status_code_high_percent_rank_dense_rank", F.dense_rank().over(w6))
    .withColumn("latency_ms_rank", F.rank().over(w7))
    .withColumn("status_code_high_scaled_dense_rank_rank", F.rank().over(w8))
)

result = (
    metrics_app_logs
    .select(
        "latency_ms",
        "status_code_high_scaled",
        "status_code_high_percent_rank",
        "has_fault_dense_rank",
        "status_code_high_scaled_rank",
        "has_fault_dense_rank_2",
        "status_code_high_scaled_dense_rank",
        "status_code_high_scaled_dense_rank_rank",
    )
    .orderBy(F.col("latency_ms").asc_nulls_last())
)
