from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("key", "level").orderBy(F.col("flag_bit8").desc_nulls_last())
w2 = Window.partitionBy("key").orderBy(F.col("latency_ms").desc_nulls_last())
w3 = Window.partitionBy("level").orderBy(F.col("status_code_high").desc())
w4 = Window.orderBy(F.col("flag_bit8").desc_nulls_first())
w5 = Window.partitionBy("level")
w6 = Window.partitionBy("level").orderBy(F.col("latency_ms_rank").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

parsed = (
    exploded
    .select(
        "level",
        "latency_ms",
        "key",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
    )
)

kept_keys = (
    parsed
    .select("level")
    .intersectAll(
        df_app_logs
        .filter(F.col("latency_ms").between(1545, 2083))
        .select("level")
    )
)

verified = (
    parsed
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .filter(F.col("flag_bit8") == 1)
    .select(
        "level",
        "value",
        "flag_bit8",
        F.percent_rank().over(w_recent).alias("flag_bit8_percent_rank"),
        F.rank().over(w2).alias("latency_ms_rank"),
        F.dense_rank().over(w3).alias("status_code_high_dense_rank"),
    )
)

remaining_keys = (
    with_windows
    .select("level")
    .intersect(
        df_app_logs
        .filter(F.col("status_code").between(308, 442))
        .select("level")
    )
)

verified_app_logs = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

scored = (
    verified_app_logs
    .withColumn("flag_bit8_dense_rank", F.dense_rank().over(w4))
    .withColumn("latency_ms_rank_share", F.round((F.col("latency_ms_rank") / F.sum("latency_ms_rank").over(w5)), 4))
)

scored_app_logs = (
    scored
    .select(
        "value",
        "flag_bit8_percent_rank",
        "status_code_high_dense_rank",
        "flag_bit8_dense_rank",
        "latency_ms_rank_share",
        F.dense_rank().over(w6).alias("latency_ms_rank_dense_rank"),
        F.rank().over(w6).alias("latency_ms_rank_rank"),
    )
)

result = (
    scored_app_logs
    .select(
        "status_code_high_dense_rank",
        "latency_ms_rank_share",
        "latency_ms_rank_dense_rank",
    )
    .orderBy(F.col("latency_ms_rank_share").desc())
)
