from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("flag_bit8").desc_nulls_first())
w_hist = Window.partitionBy("attr_key").orderBy(F.col("status_code_high").asc_nulls_first())
w3 = Window.partitionBy("attr_key")
w4 = Window.orderBy(F.col("flag_bit8").asc_nulls_first())
w5 = Window.partitionBy("host").orderBy(F.col("flag_bit8").desc_nulls_first())
w6 = Window.partitionBy("level").orderBy(F.col("flag_bit8_rank").desc_nulls_last())
w7 = Window.partitionBy("level")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

unpacked = (
    flattened
    .select(
        "id",
        "host",
        "level",
        "attr_key",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
    )
)

metrics = (
    unpacked
    .filter(F.col("host").isin(["h3", "h4"]))
    .select(
        "id",
        "level",
        "v",
        "status_code_high",
        "flag_bit8",
        F.dense_rank().over(w).alias("flag_bit8_dense_rank"),
        F.dense_rank().over(w_hist).alias("status_code_high_dense_rank"),
        F.avg("status_code_high").over(w3).alias("attr_key_avg_status_code_high"),
        F.percent_rank().over(w4).alias("flag_bit8_percent_rank"),
        F.rank().over(w5).alias("flag_bit8_rank"),
    )
)

base = (
    metrics
    .filter(F.col("status_code_high_dense_rank") == 6)
    .withColumn("status_code_high_per_flag_bit8_dense_rank", F.round((F.col("status_code_high") / F.col("flag_bit8_dense_rank")), 2))
)

cleaned = (
    base
    .filter(F.col("status_code_high_dense_rank").isNotNull())
    .select(
        "id",
        "level",
        "v",
        "flag_bit8",
        "flag_bit8_dense_rank",
        "status_code_high_dense_rank",
        "flag_bit8_percent_rank",
        "flag_bit8_rank",
        "status_code_high_per_flag_bit8_dense_rank",
        F.length("level").alias("level_length"),
        F.trim("level").alias("clean_level"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("flag_bit8") == 1)
    .select(
        "*",
        (F.col("flag_bit8_dense_rank") / F.nullif("status_code_high_per_flag_bit8_dense_rank", F.lit(0))).alias("flag_bit8_dense_rank_per_status_code_high_per_flag_bit8_dense_rank"),
    )
)

with_windows = (
    cleaned_app_logs
    .withColumn("flag_bit8_rank_rank", F.rank().over(w6))
    .withColumn("level_avg_status_code_high_dense_rank", F.avg("status_code_high_dense_rank").over(w7))
    .withColumn("level_sum_status_code_high_per_flag_bit8_dense_rank", F.sum("status_code_high_per_flag_bit8_dense_rank").over(w7))
)

result = (
    with_windows
    .select(
        "id",
        "flag_bit8",
        "status_code_high_dense_rank",
        "flag_bit8_percent_rank",
        "flag_bit8_rank",
        "level_length",
        "flag_bit8_dense_rank_per_status_code_high_per_flag_bit8_dense_rank",
        "level_avg_status_code_high_dense_rank",
        "level_sum_status_code_high_per_flag_bit8_dense_rank",
    )
    .orderBy("level_length")
)
