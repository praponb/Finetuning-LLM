from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("k").orderBy("running_avg_is_flagged", "id", "k")
w3 = Window.partitionBy("k").orderBy("running_max_is_flagged", "id", "k")
w4 = Window.partitionBy("k").orderBy("running_max_is_flagged", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

parsed = (
    exploded
    .filter(F.col("log_ts") >= "2023-07-13")
    .select(
        "id",
        "log_ts",
        "k",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = (
    parsed
    .select(
        "id",
        "k",
        F.max("is_flagged").over(w).alias("running_max_is_flagged"),
        F.avg("is_flagged").over(w).alias("running_avg_is_flagged"),
        F.count("is_flagged").over(w).alias("running_count_is_flagged"),
    )
)

with_windows = (
    metrics
    .filter(F.col("k").isin(["trace", "version", "region"]))
    .withColumn("prev_running_avg_is_flagged", F.lag("running_avg_is_flagged").over(w2))
    .withColumn("next_running_avg_is_flagged", F.lead("running_avg_is_flagged").over(w2))
    .withColumn("next_running_count_is_flagged", F.lead("running_count_is_flagged").over(w2))
    .withColumn("seq_no", F.row_number().over(w2))
)

metrics_app_logs = (
    with_windows
    .withColumn("running_max_is_flagged_bucket", F.ntile(10).over(w3))
    .withColumn("running_seq_no", F.sum("seq_no").over(w4))
)

result = (
    metrics_app_logs
    .groupBy("seq_no")
    .agg(
        F.percentile_approx("next_running_avg_is_flagged", 0.5).alias("next_running_avg_is_flagged_p50"),
        F.count(F.when(F.col("k") == "region", 1)).alias("region_rows"),
        F.percentile_approx("running_max_is_flagged", 0.9).alias("running_max_is_flagged_p90"),
        F.size(F.collect_set("k")).alias("num_ks"),
    )
    .orderBy(F.col("region_rows").asc_nulls_last())
)
