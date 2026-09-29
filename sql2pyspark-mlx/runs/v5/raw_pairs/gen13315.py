from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("key")
w_hist = Window.partitionBy("key").orderBy(F.col("key_avg_is_flagged").desc_nulls_first())
w3 = Window.partitionBy("key").orderBy(F.col("key_max_is_flagged").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

unpacked = (
    flattened
    .select(
        "latency_ms",
        "key",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = unpacked.select("*", F.avg("is_flagged").over(w_part).alias("key_avg_is_flagged"))

scored = (
    with_windows
    .select(
        "*",
        F.rank().over(w_hist).alias("key_avg_is_flagged_rank"),
        F.max("is_flagged").over(w_part).alias("key_max_is_flagged"),
    )
)

scored_app_logs = (
    scored
    .select(
        "key",
        "key_avg_is_flagged",
        F.percent_rank().over(w3).alias("key_max_is_flagged_percent_rank"),
        F.stddev_samp("key_avg_is_flagged_rank").over(w_part).alias("key_stddev_samp_key_avg_is_flagged_rank"),
    )
)

result = (
    scored_app_logs
    .select(
        "key",
        "key_avg_is_flagged",
        "key_max_is_flagged_percent_rank",
        "key_stddev_samp_key_avg_is_flagged_rank",
    )
    .orderBy(F.col("key_stddev_samp_key_avg_is_flagged_rank").desc(), "key")
)
