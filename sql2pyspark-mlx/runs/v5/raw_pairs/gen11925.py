from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

unpacked = (
    lines
    .select(
        "id",
        "host",
        "level",
        "k",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = unpacked.withColumn("host_max_status_code_high", F.max("status_code_high").over(w))

cleaned = (
    metrics
    .filter(F.col("level").isin(["ERROR", "WARN"]))
    .select(
        "id",
        "host",
        "level",
        "k",
        "flag_bit8",
        "status_code_high",
        "host_max_status_code_high",
        F.concat_ws("-", "k", "level").alias("k_level_key"),
    )
)

grouped = (
    cleaned
    .groupBy(
        F.when(F.col("status_code_high") >= 2, "PREMIUM").otherwise("STANDARD").alias("status_code_high_tier"),
    )
    .agg(
        F.max(F.when(F.col("host") == "h4", F.col("host_max_status_code_high"))).alias("h4_max_host_max_status_code_high"),
        F.countDistinct("level").alias("distinct_levels"),
        F.round(F.sum("flag_bit8") / F.nullif(F.count(F.when(F.col("k") == "region", 1)), F.lit(0)), 2).alias("flag_bit8_per_region"),
    )
    .filter(F.col("distinct_levels") > 1)
)

result = (
    grouped
    .select("status_code_high_tier", "h4_max_host_max_status_code_high")
    .orderBy(F.col("h4_max_host_max_status_code_high").desc_nulls_last())
)
