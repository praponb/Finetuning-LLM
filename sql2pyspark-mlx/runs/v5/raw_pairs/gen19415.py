from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("level").orderBy("status_code", "id", "attr_key")
w_running = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key")
w_recent = Window.partitionBy("attr_key").orderBy("next_latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level").orderBy(F.col("latency_ms").desc())
w5 = Window.partitionBy("attr_key").orderBy("running_count_prev_latency_ms", "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

with_windows = (
    exploded
    .filter(F.col("status_code") == 431)
    .withColumn("next_latency_ms", F.lead("latency_ms").over(w_part))
    .withColumn("prev_latency_ms", F.lag("latency_ms").over(w_part))
    .withColumn("prev_status_code", F.lag("status_code").over(w_part))
    .withColumn("next_status_code", F.lead("status_code").over(w_part))
    .withColumn("seq_no", F.row_number().over(w_part))
    .withColumn("prev_latency_ms_2", F.lag("latency_ms", 1).over(w_running))
)

metrics = (
    with_windows
    .select(
        "id",
        "latency_ms",
        "status_code",
        "attr_key",
        "attr_value",
        "next_latency_ms",
        "prev_latency_ms_2",
        F.sum("prev_latency_ms").over(w_recent).alias("running_sum_prev_latency_ms"),
        F.avg("prev_latency_ms").over(w_recent).alias("running_avg_prev_latency_ms"),
        F.count("prev_latency_ms").over(w_recent).alias("running_count_prev_latency_ms"),
        F.cume_dist().over(w4).alias("latency_ms_cume_dist"),
    )
)

latest = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "latency_ms",
        "status_code",
        "attr_key",
        "prev_latency_ms_2",
        "running_avg_prev_latency_ms",
        "running_count_prev_latency_ms",
        "latency_ms_cume_dist",
        "rank_in_group",
        F.when(F.col("attr_key").rlike("^(trace|region)$"), "GROUP_B").otherwise("PRIORITY").alias("attr_key_zone"),
    )
    .orderBy(
        F.col("latency_ms_cume_dist").desc_nulls_last(),
        F.col("running_count_prev_latency_ms").desc(),
    )
)
