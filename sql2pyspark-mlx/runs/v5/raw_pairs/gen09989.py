from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("flag_bit4").asc_nulls_last())
w2 = Window.partitionBy("host").orderBy(F.col("status_code_high").desc_nulls_last())
w3 = Window.partitionBy("host")
w4 = Window.partitionBy("host").orderBy(F.col("flag_bit4").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

unpacked = (
    exploded
    .filter(F.col("host").isin(["h1", "h4"]))
    .select(
        "id",
        "host",
        "status_code",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    unpacked
    .select(
        "id",
        "status_code_high",
        F.cume_dist().over(w).alias("flag_bit4_cume_dist"),
        F.rank().over(w2).alias("status_code_high_rank"),
        F.stddev_samp("status_code_high").over(w3).alias("host_stddev_samp_status_code_high"),
        F.dense_rank().over(w4).alias("flag_bit4_dense_rank"),
    )
)

valid = (
    scored
    .filter(F.col("host_stddev_samp_status_code_high") < 1)
    .select(
        "status_code_high",
        "flag_bit4_cume_dist",
        "status_code_high_rank",
        "flag_bit4_dense_rank",
        (F.col("status_code_high_rank") / F.nullif("status_code_high", F.lit(0))).alias("status_code_high_rank_per_status_code_high"),
        F.round((F.col("host_stddev_samp_status_code_high") / F.col("flag_bit4_dense_rank")), 2).alias("host_stddev_samp_status_code_high_per_flag_bit4_dense_rank"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("status_code_high_rank_per_status_code_high") >= 73, "LARGE").otherwise("SMALL").alias("status_code_high_rank_per_status_code_high_tier"),
    )
    .agg(
        F.min("status_code_high_rank").alias("min_status_code_high_rank"),
        F.percentile_approx("flag_bit4_dense_rank", [0.25, 0.5, 0.75]).alias("flag_bit4_dense_rank_quartiles"),
    )
    .filter(F.col("min_status_code_high_rank") >= 0)
)
