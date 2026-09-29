from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("is_flagged").desc())
w2 = Window.partitionBy("attr_key").orderBy(F.col("status_code_high").desc_nulls_last())
w3 = Window.partitionBy("level", "service")
w4 = Window.partitionBy("attr_key").orderBy("is_flagged_dense_rank", "id", "attr_key").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("attr_key").orderBy("status_code_high_percent_rank", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("attr_key").orderBy("last7_avg_status_code_high_percent_rank", "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

flattened_attrs = (
    flattened
    .select(
        "id",
        "service",
        "level",
        "attr_key",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
    )
)

with_windows = (
    flattened_attrs
    .withColumn("is_flagged_dense_rank", F.dense_rank().over(w))
    .withColumn("status_code_high_percent_rank", F.percent_rank().over(w2))
    .withColumn("level_min_is_flagged", F.min("is_flagged").over(w3))
)

with_windows_app_logs = (
    with_windows
    .select(
        "*",
        F.sum("status_code_high_percent_rank").over(w4).alias("last7_sum_status_code_high_percent_rank"),
        F.avg("status_code_high_percent_rank").over(w4).alias("last7_avg_status_code_high_percent_rank"),
        F.max("status_code_high_percent_rank").over(w4).alias("last7_max_status_code_high_percent_rank"),
        F.first("level_min_is_flagged", ignorenulls=True).over(w5).alias("first_known_level_min_is_flagged"),
    )
)

enriched = (
    with_windows_app_logs
    .filter(F.col("status_code_high") > 1)
    .withColumn("next_is_flagged", F.lead("is_flagged").over(w6))
    .withColumn("prev_is_flagged", F.lag("is_flagged").over(w6))
    .withColumn("prev_level_min_is_flagged", F.lag("level_min_is_flagged").over(w6))
    .withColumn("event_seq", F.row_number().over(w6))
)

result = (
    enriched
    .groupBy(
        "level",
        F.when(F.col("is_flagged_dense_rank") >= 9, "HIGH").otherwise("LOW").alias("is_flagged_dense_rank_tier"),
    )
    .agg(
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count_if(F.col("level") == "ERROR").alias("error_events"),
        F.percentile_approx("last7_avg_status_code_high_percent_rank", [0.25, 0.5, 0.75]).alias("last7_avg_status_code_high_percent_rank_quartiles"),
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.percentile_approx("event_seq", 0.95).alias("event_seq_p95"),
    )
    .filter(F.col("error_events") < 1000)
    .orderBy(F.col("event_seq_p95").desc_nulls_last())
)
