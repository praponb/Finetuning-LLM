from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("level").orderBy(F.col("status_code_high").asc_nulls_first())
w3 = Window.partitionBy("k").orderBy(F.col("status_code_high_cume_dist").asc_nulls_first())
w_grp = Window.partitionBy("k")
w_recent = Window.partitionBy("k").orderBy(F.col("status_code_high_cume_dist").desc_nulls_first())
w6 = Window.orderBy(F.col("is_flagged").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

flattened_attrs = (
    exploded
    .filter(F.col("level").isin(["ERROR", "DEBUG", "INFO"]))
    .select(
        "level",
        "k",
        "v",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    flattened_attrs
    .select(
        "k",
        "v",
        "is_flagged",
        F.avg("status_code_high").over(w).alias("level_avg_status_code_high"),
        F.cume_dist().over(w2).alias("status_code_high_cume_dist"),
    )
)

scored = (
    with_windows
    .select(
        "level_avg_status_code_high",
        "status_code_high_cume_dist",
        F.dense_rank().over(w3).alias("status_code_high_cume_dist_dense_rank"),
        F.count("is_flagged").over(w_grp).alias("k_count_is_flagged"),
        F.sum("level_avg_status_code_high").over(w_grp).alias("k_sum_level_avg_status_code_high"),
        F.dense_rank().over(w_recent).alias("status_code_high_cume_dist_dense_rank_2"),
        F.dense_rank().over(w6).alias("is_flagged_dense_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("status_code_high_cume_dist_dense_rank_2") >= 7, "HIGH").otherwise("LOW").alias("status_code_high_cume_dist_dense_rank_2_tier"),
        F.when(F.col("k_count_is_flagged") >= 16, "PREMIUM").otherwise("STANDARD").alias("k_count_is_flagged_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
        F.max("status_code_high_cume_dist_dense_rank").alias("max_status_code_high_cume_dist_dense_rank"),
    )
    .orderBy(F.col("cnt_2").asc_nulls_last())
)
