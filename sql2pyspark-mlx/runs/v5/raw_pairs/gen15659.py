from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code")
w2 = Window.orderBy(F.col("status_code_high").desc())
w3 = Window.orderBy("status_code_high")
w4 = Window.orderBy(F.col("status_code_high").desc_nulls_first())
w5 = Window.orderBy(F.col("has_fault").asc_nulls_last())
w6 = Window.orderBy(F.col("status_code_high_rank_minus_has_fault").desc_nulls_first())
w7 = Window.orderBy(F.col("status_code_high_rank_minus_has_fault").asc_nulls_first())
w8 = Window.orderBy(F.col("status_code_high_percent_rank").asc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    exploded
    .select(
        "latency_ms",
        "status_code",
        "attr_value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    parsed
    .filter(F.col("has_fault") <= 1)
    .select(
        "has_fault",
        "status_code_high",
        F.cume_dist().over(w).alias("status_code_cume_dist"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.dense_rank().over(w2).alias("status_code_high_dense_rank"),
        F.percent_rank().over(w3).alias("status_code_high_percent_rank"),
        F.rank().over(w4).alias("status_code_high_rank"),
        F.dense_rank().over(w5).alias("has_fault_dense_rank"),
    )
)

cleaned = (
    scored
    .filter(F.col("status_code_high_percent_rank") > 0.37)
    .select(
        "status_code_cume_dist",
        "status_code_high_dense_rank",
        "status_code_high_percent_rank",
        "status_code_high_rank",
        "has_fault_dense_rank",
        (F.col("has_fault") / F.nullif("status_code_cume_dist", F.lit(0))).alias("has_fault_per_status_code_cume_dist"),
        (F.col("status_code_high_rank") - F.col("has_fault")).alias("status_code_high_rank_minus_has_fault"),
    )
)

result = (
    cleaned
    .select(
        "status_code_cume_dist",
        "status_code_high_rank",
        "has_fault_dense_rank",
        "has_fault_per_status_code_cume_dist",
        F.cume_dist().over(w6).alias("status_code_high_rank_minus_has_fault_cume_dist"),
        F.percent_rank().over(w7).alias("status_code_high_rank_minus_has_fault_percent_rank"),
        F.dense_rank().over(w8).alias("status_code_high_percent_rank_dense_rank"),
    )
)
