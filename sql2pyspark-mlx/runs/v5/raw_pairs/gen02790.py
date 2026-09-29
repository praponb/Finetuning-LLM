from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "k").orderBy("status_code_high")
w2 = Window.partitionBy("k")
w3 = Window.orderBy(F.col("status_code_high").desc_nulls_first())
w4 = Window.partitionBy("k").orderBy(F.col("is_flagged_adjusted").desc_nulls_first())
w5 = Window.orderBy(F.col("k_count_status_code_high").desc_nulls_last())
w6 = Window.partitionBy("k").orderBy(F.col("is_flagged_adjusted").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

parsed = (
    flattened
    .select(
        "id",
        "host",
        "k",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    parsed
    .filter(F.col("k").isNotNull() & (F.col("k") != "version"))
    .select(
        "id",
        "host",
        "k",
        "is_flagged",
        "status_code_high",
        F.round((F.col("is_flagged") * 0.85), 2).alias("is_flagged_adjusted"),
        F.coalesce("k", F.lit("NONE")).alias("resolved_k"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.percent_rank().over(w).alias("status_code_high_percent_rank"),
        F.count("status_code_high").over(w2).alias("k_count_status_code_high"),
        F.min("is_flagged_adjusted").over(w2).alias("k_min_is_flagged_adjusted"),
    )
)

base = (
    scored
    .filter(F.col("is_flagged_adjusted") <= 25)
    .select(
        "id",
        "k",
        "is_flagged",
        "status_code_high",
        "is_flagged_adjusted",
        "resolved_k",
        "status_code_high_percent_rank",
        "k_count_status_code_high",
        "k_min_is_flagged_adjusted",
        F.least("status_code_high_percent_rank", "k_count_status_code_high").alias("least_status_code_high_percent_rank_k_count_status_code_high"),
        F.coalesce("k", F.lit("DIRECT")).alias("resolved_k_2"),
    )
)

result = (
    base
    .select(
        "id",
        "k",
        "is_flagged_adjusted",
        "status_code_high_percent_rank",
        "k_min_is_flagged_adjusted",
        "least_status_code_high_percent_rank_k_count_status_code_high",
        "resolved_k_2",
        F.rank().over(w3).alias("status_code_high_rank"),
        F.percent_rank().over(w4).alias("is_flagged_adjusted_percent_rank"),
        F.cume_dist().over(w5).alias("k_count_status_code_high_cume_dist"),
        F.avg("status_code_high_percent_rank").over(w2).alias("k_avg_status_code_high_percent_rank"),
        F.rank().over(w6).alias("is_flagged_adjusted_rank"),
    )
)
