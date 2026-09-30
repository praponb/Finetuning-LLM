from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("level").orderBy(F.col("status_code_adjusted").desc(), "id", "key")
w2 = Window.orderBy("status_code_adjusted", "id", "key").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("host", "key").orderBy("log_ts", "id", "key")
w4 = Window.partitionBy("key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

base = (
    lines
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .withColumn("status_code_adjusted", F.round((F.col("status_code") * 1.15), 2))
    .withColumn("level_prefix", F.substring("level", 1, 3))
)

with_windows = (
    base
    .select(
        "*",
        F.row_number().over(w_running).alias("seq_no"),
        F.avg("status_code").over(w2).alias("status_code_avg_5"),
    )
)

scored = (
    with_windows
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("key_avg_status_code_avg_5", F.avg("status_code_avg_5").over(w4))
)

result = (
    scored
    .groupBy(
        F.when(F.col("status_code_avg_5") >= 429, "PREMIUM").otherwise("STANDARD").alias("status_code_avg_5_tier"),
    )
    .agg(
        F.countDistinct("key").alias("distinct_keys"),
        F.count(F.when(F.col("key") == "region", 1)).alias("region_count"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("level") == "DEBUG", 1)), F.lit(0)), 2).alias("seq_no_per_debug"),
    )
    .filter(F.col("seq_no_per_debug") > 0)
    .orderBy(F.col("status_code_avg_5_tier").desc())
)
