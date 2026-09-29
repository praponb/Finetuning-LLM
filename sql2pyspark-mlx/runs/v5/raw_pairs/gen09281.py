from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w_recent = Window.partitionBy("k").orderBy("status_code")
w_grp = Window.partitionBy("k").orderBy(F.col("host_stddev_samp_status_code").desc())
w4 = Window.partitionBy("k")
w5 = Window.partitionBy("k").orderBy(F.col("has_fault").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

flattened_attrs = (
    exploded
    .filter(F.hour("log_ts").between(7, 12))
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "k",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
    )
)

metrics = (
    flattened_attrs
    .filter(F.col("status_code_high") < 1)
    .withColumn("host_stddev_samp_status_code", F.stddev_samp("status_code").over(w))
    .withColumn("host_sum_status_code_high", F.sum("status_code_high").over(w))
    .withColumn("host_count_status_code_high", F.count("status_code_high").over(w))
)

valid = (
    metrics
    .filter(F.col("k") != "trace")
    .select(
        "id",
        "log_ts",
        "status_code",
        "k",
        "v",
        "has_fault",
        "host_stddev_samp_status_code",
        F.date_format("log_ts", "MMM yyyy").alias("log_ts_label"),
    )
)

base = (
    valid
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & (~F.col("k").eqNullSafe(F.lit("trace"))))
    .withColumn("status_code_level", F.when(F.col("status_code") >= 423, "High").when(F.col("status_code") >= 304, "Medium").otherwise("Low"))
)

result = (
    base
    .select(
        "id",
        "log_ts",
        "k",
        "v",
        "has_fault",
        "host_stddev_samp_status_code",
        "log_ts_label",
        F.percent_rank().over(w_recent).alias("status_code_percent_rank"),
        F.percent_rank().over(w_grp).alias("host_stddev_samp_status_code_percent_rank"),
        F.round((F.col("host_stddev_samp_status_code") / F.sum("host_stddev_samp_status_code").over(w4)), 4).alias("host_stddev_samp_status_code_share"),
        F.percent_rank().over(w_recent).alias("status_code_percent_rank_2"),
        F.cume_dist().over(w5).alias("has_fault_cume_dist"),
    )
)
