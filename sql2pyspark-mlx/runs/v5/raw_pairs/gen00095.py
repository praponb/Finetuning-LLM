from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("status_code_high").desc())
w_recent = Window.orderBy(F.col("status_code_high").desc_nulls_first())
w3 = Window.orderBy("status_code_high")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

parsed = (
    flattened
    .filter(F.col("status_code").between(291, 409))
    .select(
        "id",
        "status_code",
        "v",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

base = (
    parsed
    .filter(F.col("status_code_high") < 2)
    .select(
        "*",
        F.round("status_code_high", 2).alias("status_code_high_rounded"),
        (F.col("status_code_high") / F.nullif("has_fault", F.lit(0))).alias("status_code_high_per_has_fault"),
    )
)

with_windows = (
    base
    .filter(F.col("status_code_high_per_has_fault") >= 32)
    .select(
        "id",
        "v",
        "has_fault",
        "status_code_high",
        "status_code_high_rounded",
        "status_code_high_per_has_fault",
        F.rank().over(w_grp).alias("status_code_high_rank"),
        F.cume_dist().over(w_recent).alias("status_code_high_cume_dist"),
        F.cume_dist().over(w3).alias("status_code_high_cume_dist_2"),
        F.cume_dist().over(w_recent).alias("status_code_high_cume_dist_3"),
    )
)

result = (
    with_windows
    .rollup(
        F.when(F.col("status_code_high_rounded") >= 89, "PREMIUM").otherwise("STANDARD").alias("status_code_high_rounded_tier"),
        F.when(F.col("status_code_high_cume_dist_2") >= 0.9, "LARGE").otherwise("SMALL").alias("status_code_high_cume_dist_2_tier"),
    )
    .agg(
        F.round(F.avg("status_code_high_rank"), 2).alias("avg_status_code_high_rank"),
        F.percentile_approx("status_code_high_rank", 0.5).alias("status_code_high_rank_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("has_fault", 0.95).alias("has_fault_p95"),
        F.round(F.avg("status_code_high"), 2).alias("avg_status_code_high"),
    )
    .orderBy(F.col("status_code_high_cume_dist_2_tier").desc())
)
