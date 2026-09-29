from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("has_fault", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("k").orderBy(F.col("has_fault").desc_nulls_first())
w3 = Window.partitionBy("k").orderBy(F.col("has_fault").asc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

unpacked = (
    flattened
    .filter(F.col("k") != "trace")
    .select(
        "id",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

scored = (
    unpacked
    .withColumn("cumulative_status_code_high", F.sum("status_code_high").over(w))
    .withColumn("has_fault_rank", F.rank().over(w_recent))
    .withColumn("has_fault_rank_2", F.rank().over(w3))
)

base = (
    scored
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "k",
        "status_code_high",
        "has_fault",
        "cumulative_status_code_high",
        "has_fault_rank",
        "has_fault_rank_2",
        F.round((F.col("has_fault_rank_2") * 1.07), 2).alias("has_fault_rank_2_scaled"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("has_fault_rank") >= 9, "LARGE").otherwise("SMALL").alias("has_fault_rank_tier"),
        F.when(F.col("cumulative_status_code_high") >= 10, "PREMIUM").otherwise("STANDARD").alias("cumulative_status_code_high_tier"),
        "k",
    )
    .agg(
        F.count(F.when(F.col("k") == "trace", 1)).alias("trace_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("k") == "version", F.col("status_code_high")).otherwise(0)).alias("version_status_code_high"),
    )
    .orderBy(F.col("has_fault_rank_tier").desc())
)
