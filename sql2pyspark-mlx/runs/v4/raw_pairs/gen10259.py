from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code_high").desc_nulls_last())
w2 = Window.partitionBy("level").orderBy(F.col("has_fault").asc_nulls_first())
w3 = Window.partitionBy("level").orderBy(F.col("has_fault_percent_rank").desc_nulls_first())
w4 = Window.partitionBy("level")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

unpacked = (
    flattened
    .select(
        "id",
        "level",
        "status_code",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    unpacked
    .withColumn("status_code_high_rank", F.rank().over(w))
    .withColumn("has_fault_percent_rank", F.percent_rank().over(w2))
)

filtered = (
    enriched
    .filter(F.col("status_code_high_rank").between(4, 8))
    .select(
        "id",
        "level",
        "status_code_high_rank",
        "has_fault_percent_rank",
        (F.col("has_fault") + F.col("status_code_high")).alias("has_fault_plus_status_code_high"),
        F.coalesce("status_code_high_rank", F.lit(0)).alias("status_code_high_rank_filled"),
    )
)

base = (
    filtered
    .filter(F.col("level") == "WARN")
    .withColumn("has_fault_plus_status_code_high_per_status_code_high_rank_filled", (F.col("has_fault_plus_status_code_high") / F.nullif("status_code_high_rank_filled", F.lit(0))))
)

scored = (
    base
    .withColumn("has_fault_percent_rank_percent_rank", F.percent_rank().over(w3))
    .withColumn("level_max_has_fault_plus_status_code_high_per_status_code_high_rank_filled", F.max("has_fault_plus_status_code_high_per_status_code_high_rank_filled").over(w4))
)

valid = (
    scored
    .filter(F.col("status_code_high_rank_filled") <= 65)
    .withColumn("has_fault_percent_rank_percent_rank_int", F.col("has_fault_percent_rank_percent_rank").cast("int"))
)

result = (
    valid
    .select(
        "id",
        "has_fault_plus_status_code_high",
        "status_code_high_rank_filled",
        "has_fault_plus_status_code_high_per_status_code_high_rank_filled",
        "has_fault_percent_rank_percent_rank",
        "level_max_has_fault_plus_status_code_high_per_status_code_high_rank_filled",
        "has_fault_percent_rank_percent_rank_int",
    )
    .orderBy(
        F.col("has_fault_percent_rank_percent_rank_int").desc_nulls_first(),
        "has_fault_percent_rank_percent_rank",
    )
)
