from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "level").orderBy(F.col("status_code_high").desc_nulls_first())
w2 = Window.partitionBy("k")
w3 = Window.partitionBy("k").orderBy("status_code")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "id",
        "level",
        "status_code",
        "k",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

matched_keys = (
    flattened_attrs
    .select("level")
    .subtract(
        df_app_logs
        .filter(F.col("status_code").between(293, 360))
        .select("level")
    )
)

verified = (
    flattened_attrs
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .filter(F.col("flag_bit8") < 0)
    .select("*", F.dense_rank().over(w).alias("status_code_high_dense_rank"))
)

scored = (
    metrics
    .select(
        "*",
        F.sum("flag_bit8").over(w2).alias("k_sum_flag_bit8"),
        F.cume_dist().over(w3).alias("status_code_cume_dist"),
    )
)

base = (
    scored
    .filter(F.col("flag_bit8") <= 0)
    .select(
        "status_code",
        "k",
        "status_code_high",
        "status_code_high_dense_rank",
        "k_sum_flag_bit8",
        F.length("level").alias("level_length"),
    )
)

result = (
    base
    .groupBy("k")
    .agg(
        F.round(F.sum("k_sum_flag_bit8") / F.nullif(F.count(F.when(F.col("k") == "version", 1)), F.lit(0)), 2).alias("k_sum_flag_bit8_per_version"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
    )
    .filter(F.col("k_sum_flag_bit8_per_version") < 1000000)
)
