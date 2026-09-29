from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w_recent = Window.partitionBy("k").orderBy("status_code_high")
w3 = Window.partitionBy("k").orderBy(F.col("status_code_high").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

flattened_attrs = (
    lines
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "k",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
    )
)

metrics = (
    flattened_attrs
    .filter(F.col("status_code_high") >= 1)
    .select(
        "log_ts",
        "k",
        "attr_value",
        "status_code_high",
        F.min("flag_bit8").over(w).alias("k_min_flag_bit8"),
        F.percent_rank().over(w_recent).alias("status_code_high_percent_rank"),
    )
)

scored = (
    metrics
    .select(
        "log_ts",
        "k",
        "attr_value",
        "k_min_flag_bit8",
        F.sum("status_code_high").over(w).alias("k_sum_status_code_high"),
        F.cume_dist().over(w3).alias("status_code_high_cume_dist"),
    )
)

base = (
    scored
    .filter((~F.col("k").eqNullSafe(F.lit("version"))) & (F.col("k_min_flag_bit8") <= 1))
    .select(
        "*",
        (F.col("status_code_high_cume_dist") / F.nullif("k_min_flag_bit8", F.lit(0))).alias("status_code_high_cume_dist_per_k_min_flag_bit8"),
    )
)

result = (
    base
    .select(
        "k",
        "status_code_high_cume_dist_per_k_min_flag_bit8",
        "status_code_high_cume_dist",
    )
    .unpivot(
        ids=["k"],
        values=["status_code_high_cume_dist_per_k_min_flag_bit8", "status_code_high_cume_dist"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
