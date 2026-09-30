from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("has_fault").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

parsed = (
    flattened
    .filter(F.col("k") != "region")
    .select(
        "id",
        "level",
        "status_code",
        "k",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

ranked = (
    parsed
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("status_code") >= 393, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_count"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.sum(F.when(F.col("level") == "INFO", F.col("status_code")).otherwise(0)).alias("info_status_code"),
    )
    .orderBy("info_status_code")
)
