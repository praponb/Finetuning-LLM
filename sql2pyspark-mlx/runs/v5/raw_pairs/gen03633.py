from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("level").orderBy("status_code_high", "id", "key")
w2 = Window.orderBy("next_latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level", "host").orderBy(F.col("next_is_flagged").desc_nulls_last())
w4 = Window.partitionBy("key").orderBy("row_seq", "id", "key")
w5 = Window.partitionBy("key").orderBy(F.col("status_code_high").desc(), "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

unpacked = (
    exploded
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
    )
)

with_windows = (
    unpacked
    .withColumn("next_is_flagged", F.lead("is_flagged").over(w_recent))
    .withColumn("next_latency_ms", F.lead("latency_ms").over(w_recent))
    .withColumn("row_seq", F.row_number().over(w_recent))
)

metrics = (
    with_windows
    .filter(F.col("host") == "h2")
    .select(
        "id",
        "key",
        "status_code_high",
        "row_seq",
        F.last("key", ignorenulls=True).over(w2).alias("last_known_key"),
        F.cume_dist().over(w3).alias("next_is_flagged_cume_dist"),
    )
)

with_windows_app_logs = (
    metrics
    .withColumn("prev_row_seq", F.lag("row_seq").over(w4))
    .withColumn("next_next_is_flagged_cume_dist", F.lead("next_is_flagged_cume_dist").over(w4))
    .withColumn("prev_next_is_flagged_cume_dist", F.lag("next_is_flagged_cume_dist").over(w4))
    .withColumn("row_seq_2", F.row_number().over(w4))
    .withColumn("status_code_high_bucket", F.ntile(4).over(w5))
)

result = (
    with_windows_app_logs
    .groupBy(
        "key",
        F.when(F.col("prev_row_seq") >= 9, "LARGE").otherwise("SMALL").alias("prev_row_seq_tier"),
    )
    .agg(
        F.count(F.when(F.col("key") == "region", 1)).alias("region_count"),
        F.sum(F.when(F.col("key") == "region", F.col("next_next_is_flagged_cume_dist"))).alias("region_total_next_next_is_flagged_cume_dist"),
        F.percentile_approx("next_is_flagged_cume_dist", [0.25, 0.5, 0.75]).alias("next_is_flagged_cume_dist_quartiles"),
        F.percentile_approx("next_is_flagged_cume_dist", 0.5).alias("next_is_flagged_cume_dist_p50"),
    )
    .filter(F.col("region_count") > 1)
    .orderBy(F.col("region_total_next_next_is_flagged_cume_dist").desc_nulls_first())
)
