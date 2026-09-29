from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("status_code_high").desc_nulls_last())
w2 = Window.partitionBy("host")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    exploded
    .filter(F.col("host").isin(["h1", "h3"]))
    .select(
        "id",
        "host",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

with_windows = parsed.select("*", F.rank().over(w).alias("status_code_high_rank"))

filtered = (
    with_windows
    .filter(F.col("is_flagged") != 1)
    .withColumn("is_flagged_times_status_code_high_rank", (F.col("is_flagged") * F.col("status_code_high_rank")))
)

base = (
    filtered
    .filter(F.col("is_flagged") == 1)
    .withColumn("status_code_high_rank_minus_is_flagged", (F.col("status_code_high_rank") - F.col("is_flagged")))
)

with_windows_app_logs = (
    base
    .filter(F.col("host").isin(["h1", "h2"]))
    .select(
        "id",
        "is_flagged",
        "status_code_high_rank",
        "is_flagged_times_status_code_high_rank",
        "status_code_high_rank_minus_is_flagged",
        F.stddev_samp("status_code_high_rank_minus_is_flagged").over(w2).alias("host_stddev_samp_status_code_high_rank_minus_is_flagged"),
    )
)

result = (
    with_windows_app_logs
    .select(
        "id",
        "is_flagged_times_status_code_high_rank",
        "host_stddev_samp_status_code_high_rank_minus_is_flagged",
    )
    .orderBy(
        F.col("is_flagged_times_status_code_high_rank").desc_nulls_first(),
        "host_stddev_samp_status_code_high_rank_minus_is_flagged",
    )
)
