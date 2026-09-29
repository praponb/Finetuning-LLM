from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("flag_bit1_times_status_code_high")
w2 = Window.orderBy(F.col("flag_bit1").desc_nulls_first())
w3 = Window.orderBy(F.col("flag_bit1_times_status_code_high_dense_rank").desc_nulls_first())
w_hist = Window.orderBy(F.col("flag_bit1_times_status_code_high").desc_nulls_first())
w5 = Window.orderBy(F.col("flag_bit1_times_status_code_high").asc_nulls_first())
w6 = Window.orderBy(F.col("flag_bit1_times_status_code_high_dense_rank").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

parsed = (
    exploded
    .select(
        "status_code",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
    )
)

base = (
    parsed
    .filter(F.col("status_code") <= 317)
    .withColumn("flag_bit1_times_status_code_high", (F.col("flag_bit1") * F.col("status_code_high")))
)

metrics = (
    base
    .withColumn("flag_bit1_times_status_code_high_dense_rank", F.dense_rank().over(w))
    .withColumn("flag_bit1_percent_rank", F.percent_rank().over(w2))
)

with_windows = (
    metrics
    .select(
        "status_code",
        "status_code_high",
        "flag_bit1_times_status_code_high",
        "flag_bit1_times_status_code_high_dense_rank",
        "flag_bit1_percent_rank",
        F.cume_dist().over(w3).alias("flag_bit1_times_status_code_high_dense_rank_cume_dist"),
        F.rank().over(w_hist).alias("flag_bit1_times_status_code_high_rank"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("flag_bit1_times_status_code_high_rank") < 6)
    .select(
        "flag_bit1_times_status_code_high",
        "flag_bit1_times_status_code_high_dense_rank",
        "flag_bit1_percent_rank",
        "flag_bit1_times_status_code_high_dense_rank_cume_dist",
        "flag_bit1_times_status_code_high_rank",
        (F.col("flag_bit1_times_status_code_high_dense_rank") - F.col("status_code_high")).alias("flag_bit1_times_status_code_high_dense_rank_minus_status_code_high"),
    )
)

result = (
    cleaned
    .filter(F.col("flag_bit1_times_status_code_high_dense_rank_minus_status_code_high").isNotNull())
    .select(
        "flag_bit1_times_status_code_high",
        "flag_bit1_times_status_code_high_dense_rank",
        "flag_bit1_percent_rank",
        "flag_bit1_times_status_code_high_rank",
        "flag_bit1_times_status_code_high_dense_rank_minus_status_code_high",
        F.cume_dist().over(w5).alias("flag_bit1_times_status_code_high_cume_dist"),
        F.cume_dist().over(w6).alias("flag_bit1_times_status_code_high_dense_rank_cume_dist_2"),
    )
)
