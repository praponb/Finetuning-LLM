from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("host").orderBy(F.col("status_code_high").asc_nulls_first())
w_recent = Window.partitionBy("host")
w3 = Window.orderBy(F.col("host_max_status_code_high_per_status_code_high").asc_nulls_first())
w4 = Window.partitionBy("host").orderBy(F.col("host_max_status_code_high").asc_nulls_first())
w5 = Window.partitionBy("host").orderBy(F.col("flag_bit2").desc_nulls_first())
w6 = Window.partitionBy("host").orderBy(F.col("status_code_high").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

parsed = (
    exploded
    .select(
        "host",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

enriched = (
    parsed
    .select(
        "*",
        F.cume_dist().over(w_part).alias("status_code_high_cume_dist"),
        F.max("status_code_high").over(w_recent).alias("host_max_status_code_high"),
        F.sum("flag_bit2").over(w_recent).alias("host_sum_flag_bit2"),
    )
)

base = (
    enriched
    .filter(F.col("status_code_high_cume_dist") <= 0.74)
    .withColumn("host_title", F.initcap(F.trim("host")))
)

cleaned = (
    base
    .filter(F.col("host_max_status_code_high").isNotNull())
    .select(
        "host",
        "status_code_high",
        "flag_bit2",
        "status_code_high_cume_dist",
        "host_max_status_code_high",
        "host_sum_flag_bit2",
        F.trim("host").alias("clean_host"),
        (F.col("host_max_status_code_high") / F.nullif("status_code_high", F.lit(0))).alias("host_max_status_code_high_per_status_code_high"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.percent_rank().over(w3).alias("host_max_status_code_high_per_status_code_high_percent_rank"),
        F.stddev_samp("status_code_high_cume_dist").over(w_recent).alias("host_stddev_samp_status_code_high_cume_dist"),
        F.rank().over(w4).alias("host_max_status_code_high_rank"),
        F.dense_rank().over(w5).alias("flag_bit2_dense_rank"),
        F.dense_rank().over(w6).alias("status_code_high_dense_rank"),
    )
)
