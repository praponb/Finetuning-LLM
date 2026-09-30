from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("key").orderBy(F.col("latency_ms").desc_nulls_last())
w3 = Window.partitionBy("key").orderBy(F.col("status_code").desc())
w4 = Window.orderBy(F.col("status_code_high").asc_nulls_first())
w5 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.partitionBy("key")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

flattened_attrs = (
    exploded
    .filter(F.col("latency_ms") == 1090)
    .select(
        "log_ts",
        "latency_ms",
        "status_code",
        "key",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
    )
)

filtered = (
    flattened_attrs
    .filter(F.col("has_fault") != 0)
    .withColumn("status_code_high_tier", F.when(F.col("status_code_high") >= 3, "High").when(F.col("status_code_high") >= 1, "Medium").otherwise("Low"))
    .withColumn("status_code_high_times_latency_ms", (F.col("status_code_high") * F.col("latency_ms")))
    .withColumn("key_filled", F.coalesce("key", F.lit("UNKNOWN")))
)

scored = (
    filtered
    .withColumn("has_fault_avg_2h", F.avg("has_fault").over(w))
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w2))
    .withColumn("status_code_high_avg_2h", F.avg("status_code_high").over(w))
)

valid = (
    scored
    .filter((F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 05:00:00"))) & F.col("key").isNotNull())
    .withColumn("log_ts_label", F.date_format("log_ts", "yyyy-MM"))
)

result = (
    valid
    .withColumn("status_code_dense_rank", F.dense_rank().over(w3))
    .withColumn("status_code_count_2h", F.count("status_code").over(w))
    .withColumn("status_code_high_rank", F.rank().over(w4))
    .withColumn("status_code_high_times_latency_ms_avg_15m", F.avg("status_code_high_times_latency_ms").over(w5))
    .withColumn("key_avg_status_code_high_times_latency_ms", F.avg("status_code_high_times_latency_ms").over(w6))
)
