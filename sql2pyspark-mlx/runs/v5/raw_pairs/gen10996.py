from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("status_code_high").desc_nulls_last())
w_recent = Window.partitionBy("host")
w3 = Window.partitionBy("level").orderBy(F.col("status_code_high").asc_nulls_first())
w4 = Window.partitionBy("level")
w5 = Window.partitionBy("host").orderBy(F.col("status_code").desc())
w6 = Window.orderBy(F.col("status_code_high_cume_dist").desc())
w_ordered = Window.orderBy(F.col("status_code_high_dense_rank").asc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "attr_value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    parsed
    .filter(F.col("level") == "INFO")
    .select(
        "*",
        F.dense_rank().over(w).alias("status_code_high_dense_rank"),
        F.avg("has_fault").over(w_recent).alias("host_avg_has_fault"),
        F.min("has_fault").over(w_recent).alias("host_min_has_fault"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "status_code",
        "attr_value",
        "has_fault",
        "status_code_high_dense_rank",
        F.cume_dist().over(w3).alias("status_code_high_cume_dist"),
        F.avg("has_fault").over(w4).alias("level_avg_has_fault"),
        F.rank().over(w5).alias("status_code_rank"),
        F.stddev_samp("status_code_high").over(w_recent).alias("host_stddev_samp_status_code_high"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "status_code",
        "attr_value",
        "has_fault",
        "status_code_high_cume_dist",
        "level_avg_has_fault",
        "status_code_rank",
        "host_stddev_samp_status_code_high",
        F.rank().over(w6).alias("status_code_high_cume_dist_rank"),
        F.dense_rank().over(w_ordered).alias("status_code_high_dense_rank_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "status_code",
        "attr_value",
        "has_fault",
        "status_code_high_cume_dist",
        "level_avg_has_fault",
        "host_stddev_samp_status_code_high",
        "status_code_high_cume_dist_rank",
        "status_code_high_dense_rank_dense_rank",
    )
    .orderBy(F.col("status_code_high_cume_dist_rank").asc_nulls_first())
)
