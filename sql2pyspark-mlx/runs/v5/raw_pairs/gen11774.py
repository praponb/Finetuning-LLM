from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("attr_key").orderBy("flag_bit1", "id", "attr_key")
w_hist = Window.partitionBy("attr_key").orderBy("prev_flag_bit1", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("attr_key").orderBy(F.col("flag_bit1").desc(), "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy("flag_bit1_bucket")
w5 = Window.partitionBy("attr_key").orderBy(F.col("prev_flag_bit1").asc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

flattened_attrs = (
    lines
    .select(
        "id",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
    )
)

filtered = (
    flattened_attrs
    .filter(F.col("flag_bit1").isNull())
    .withColumn("clean_attr_key", F.trim("attr_key"))
)

enriched = (
    filtered
    .filter(F.col("flag_bit1") > 0)
    .withColumn("prev_flag_bit1", F.lag("flag_bit1").over(w_running))
    .withColumn("next_status_code_high", F.lead("status_code_high").over(w_running))
    .withColumn("prev_status_code_high", F.lag("status_code_high").over(w_running))
    .withColumn("event_seq", F.row_number().over(w_running))
)

with_windows = (
    enriched
    .select(
        "id",
        "attr_key",
        "flag_bit1",
        "clean_attr_key",
        "prev_flag_bit1",
        "prev_status_code_high",
        "event_seq",
        F.sum("prev_status_code_high").over(w_hist).alias("cumulative_prev_status_code_high"),
        F.ntile(5).over(w_recent).alias("flag_bit1_bucket"),
    )
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    deduped
    .withColumn("dedup_rank_2", F.rank().over(w5))
    .filter(F.col("dedup_rank_2") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("cumulative_prev_status_code_high") >= 11, "LARGE").otherwise("SMALL").alias("cumulative_prev_status_code_high_tier"),
    )
    .agg(
        F.max("prev_status_code_high").alias("max_prev_status_code_high"),
        F.round(F.sum("prev_flag_bit1") / F.nullif(F.count(F.when(F.col("attr_key") == "trace", 1)), F.lit(0)), 2).alias("prev_flag_bit1_per_trace"),
        F.count_if(F.col("attr_key") == "version").alias("version_events"),
    )
    .filter(F.col("version_events") > 1)
    .orderBy(F.col("cumulative_prev_status_code_high_tier").desc_nulls_last())
)
