from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.orderBy(F.col("has_fault").desc())
w3 = Window.partitionBy("host")
w4 = Window.partitionBy("host").orderBy("has_fault")
w5 = Window.partitionBy("host").orderBy(F.col("has_fault").desc())
w6 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w7 = Window.partitionBy("host").orderBy(F.col("has_fault_rank_2").asc_nulls_first())

lines = (
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

unpacked = (
    lines
    .filter(F.col("log_ts") >= "2024-10-17")
    .select(
        "host",
        "log_ts",
        "status_code",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

enriched = (
    unpacked
    .select(
        "*",
        F.count("status_code").over(w).alias("status_code_count_1h"),
        F.rank().over(w2).alias("has_fault_rank"),
        F.stddev_samp("status_code").over(w3).alias("host_stddev_samp_status_code"),
        F.percent_rank().over(w4).alias("has_fault_percent_rank"),
        F.rank().over(w5).alias("has_fault_rank_2"),
    )
)

enriched_app_logs = (
    enriched
    .withColumn("has_fault_cume_dist", F.cume_dist().over(w5))
    .withColumn("has_fault_rank_2_count_1h", F.count("has_fault_rank_2").over(w6))
    .withColumn("has_fault_rank_2_rank", F.rank().over(w7))
)

cleaned = (
    enriched_app_logs
    .filter(F.col("host") == "h1")
    .select(
        "*",
        (F.col("has_fault_rank_2_count_1h") / F.nullif("has_fault", F.lit(0))).alias("has_fault_rank_2_count_1h_per_has_fault"),
        F.length("host").alias("host_length"),
    )
)

result = (
    cleaned
    .select(
        "host",
        "log_ts",
        "status_code",
        "v",
        "host_stddev_samp_status_code",
        "has_fault_cume_dist",
        "has_fault_rank_2_count_1h",
        "has_fault_rank_2_rank",
        "host_length",
        F.when(F.col("host").rlike("^(h2|h1)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("host_zone"),
    )
    .orderBy("host_length")
)
