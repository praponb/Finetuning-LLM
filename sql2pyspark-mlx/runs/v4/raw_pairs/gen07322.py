from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("flag_bit2").desc(), "id", "k")
w2 = Window.partitionBy("k").orderBy(F.col("flag_bit2").asc_nulls_first())
w3 = Window.orderBy("status_code_high", "id", "k")
w4 = Window.partitionBy("k").orderBy("status_code_high", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("k")
w_hist = Window.partitionBy("k").orderBy("status_code_high_bucket", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("k_stddev_samp_status_code_high").desc())
w8 = Window.partitionBy("k").orderBy("status_code_high", "id", "k").rowsBetween(-3, Window.currentRow)
w9 = Window.orderBy(F.col("status_code_high_bucket_2").desc(), "id", "k")
w10 = Window.partitionBy("k").orderBy(F.col("trace_running_flag_bit2").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "k",
        "v",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    parsed
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("flag_bit2_dense_rank", F.dense_rank().over(w2))
    .withColumn("status_code_high_bucket", F.ntile(5).over(w3))
    .withColumn("running_flag_bit2", F.sum("flag_bit2").over(w4))
    .withColumn("k_stddev_samp_status_code_high", F.stddev_samp("status_code_high").over(w5))
)

with_windows = (
    scored
    .withColumn("last_known_running_flag_bit2", F.last("running_flag_bit2", ignorenulls=True).over(w_hist))
    .withColumn("k_stddev_samp_status_code_high_cume_dist", F.cume_dist().over(w7))
)

metrics = (
    with_windows
    .select(
        "*",
        F.avg("k_stddev_samp_status_code_high").over(w8).alias("k_stddev_samp_status_code_high_avg_3"),
        F.ntile(4).over(w3).alias("status_code_high_bucket_2"),
    )
)

enriched = (
    metrics
    .withColumn("k_stddev_samp_flag_bit2_dense_rank", F.stddev_samp("flag_bit2_dense_rank").over(w5))
    .withColumn("k_stddev_samp_last_known_running_flag_bit2", F.stddev_samp("last_known_running_flag_bit2").over(w5))
    .withColumn("seq_no", F.row_number().over(w9))
)

result = (
    enriched
    .groupBy(
        "k",
        F.when(F.col("seq_no") >= 8, "PREMIUM").otherwise("STANDARD").alias("seq_no_tier"),
        F.when(F.col("status_code_high") >= 2, "PREMIUM").otherwise("STANDARD").alias("status_code_high_tier"),
    )
    .agg(
        F.count(F.when(F.col("k") == "version", 1)).alias("version_count"),
        F.sum(F.when(F.col("k") == "trace", F.col("running_flag_bit2")).otherwise(0)).alias("trace_running_flag_bit2"),
        F.avg("k_stddev_samp_last_known_running_flag_bit2").alias("avg_k_stddev_samp_last_known_running_flag_bit2"),
    )
    .withColumn("trace_running_flag_bit2_percent_rank", F.percent_rank().over(w10))
    .orderBy(F.col("version_count").asc_nulls_last())
)
