from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("service").orderBy("status_code_high", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("service").orderBy("status_code_high", "id", "attr_key")
w3 = Window.partitionBy("service").orderBy(F.col("status_code_high").desc_nulls_last())
w4 = Window.partitionBy("service")
w5 = Window.partitionBy("attr_key").orderBy(F.col("status_code_high_rank").desc_nulls_first(), "id", "attr_key")
w6 = Window.partitionBy("attr_key").orderBy("status_code_high_rank", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

flattened_attrs = (
    lines
    .select(
        "id",
        "service",
        "attr_key",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    flattened_attrs
    .select(
        "*",
        F.sum("flag_bit8").over(w_hist).alias("running_flag_bit8"),
        F.ntile(3).over(w_recent).alias("status_code_high_bucket"),
    )
)

enriched = (
    with_windows
    .filter(F.col("status_code_high_bucket") <= 5)
    .select(
        "id",
        "attr_key",
        F.rank().over(w3).alias("status_code_high_rank"),
        F.count("running_flag_bit8").over(w4).alias("service_count_running_flag_bit8"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

metrics = (
    latest
    .withColumn("running_count_service_count_running_flag_bit8", F.count("service_count_running_flag_bit8").over(w6))
    .withColumn("running_avg_service_count_running_flag_bit8", F.avg("service_count_running_flag_bit8").over(w6))
    .withColumn("running_max_service_count_running_flag_bit8", F.max("service_count_running_flag_bit8").over(w6))
    .withColumn("cumulative_service_count_running_flag_bit8", F.sum("service_count_running_flag_bit8").over(w6))
)

base = (
    metrics
    .filter(~F.col("attr_key").eqNullSafe(F.lit("version")))
    .withColumn("status_code_high_rank_times_running_count_service_count_running_flag_bit8", (F.col("status_code_high_rank") * F.col("running_count_service_count_running_flag_bit8")))
    .withColumn("rn_per_running_avg_service_count_running_flag_bit8", F.round((F.col("rn") / F.col("running_avg_service_count_running_flag_bit8")), 2))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("NONE")))
)

result = (
    base
    .select(
        "id",
        "attr_key",
        "service_count_running_flag_bit8",
        "rn",
        "running_avg_service_count_running_flag_bit8",
        "running_max_service_count_running_flag_bit8",
        "cumulative_service_count_running_flag_bit8",
        "status_code_high_rank_times_running_count_service_count_running_flag_bit8",
        "rn_per_running_avg_service_count_running_flag_bit8",
        F.when(F.col("attr_key").rlike("^(trace|region)$"), "GROUP_A").otherwise("PRIORITY").alias("attr_key_zone"),
    )
    .orderBy(
        F.col("rn_per_running_avg_service_count_running_flag_bit8").desc_nulls_last(),
        "id",
    )
)
