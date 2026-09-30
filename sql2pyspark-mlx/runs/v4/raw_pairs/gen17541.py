from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "service").orderBy("status_code_high")
w2 = Window.partitionBy("k").orderBy(F.col("is_flagged").desc_nulls_first())
w3 = Window.partitionBy("k", "level").orderBy(F.col("is_flagged").asc_nulls_last())
w4 = Window.partitionBy("level")
w5 = Window.partitionBy("level").orderBy(F.col("status_code_high").desc_nulls_last())
w6 = Window.partitionBy("k")
w7 = Window.partitionBy("k").orderBy(F.col("k_stddev_samp_is_flagged_rank").asc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

unpacked = (
    flattened
    .filter(F.col("k") == "trace")
    .select(
        "service",
        "level",
        "k",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    unpacked
    .withColumn("status_code_high_cume_dist", F.cume_dist().over(w))
    .withColumn("is_flagged_rank", F.rank().over(w2))
    .withColumn("is_flagged_percent_rank", F.percent_rank().over(w3))
)

with_windows_app_logs = (
    with_windows
    .withColumn("level_max_is_flagged_percent_rank", F.max("is_flagged_percent_rank").over(w4))
    .withColumn("status_code_high_rank", F.rank().over(w5))
    .withColumn("k_stddev_samp_is_flagged_rank", F.stddev_samp("is_flagged_rank").over(w6))
)

valid = (
    with_windows_app_logs
    .filter(F.col("k").isNotNull())
    .select(
        "service",
        "k",
        "is_flagged",
        "status_code_high",
        "level_max_is_flagged_percent_rank",
        "k_stddev_samp_is_flagged_rank",
        (F.col("k_stddev_samp_is_flagged_rank") / F.nullif("status_code_high_cume_dist", F.lit(0))).alias("k_stddev_samp_is_flagged_rank_per_status_code_high_cume_dist"),
    )
)

filtered = (
    valid
    .filter(F.col("k").isin(["version", "region", "trace"]))
    .select(
        "k",
        "is_flagged",
        "k_stddev_samp_is_flagged_rank",
        F.initcap(F.trim("service")).alias("service_title"),
    )
)

with_windows_app_logs_2 = (
    filtered
    .withColumn("k_stddev_samp_is_flagged_rank_percent_rank", F.percent_rank().over(w7))
    .withColumn("k_stddev_samp_is_flagged_rank_share", F.round((F.col("k_stddev_samp_is_flagged_rank") / F.sum("k_stddev_samp_is_flagged_rank").over(w6)), 4))
)

result = (
    with_windows_app_logs_2
    .select("is_flagged", "service_title", "k_stddev_samp_is_flagged_rank_share")
)
