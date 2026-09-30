from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("status_code_high").desc(), "id", "k")
w2 = Window.partitionBy("k").orderBy("has_fault", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy("has_fault", "id", "k")
w4 = Window.partitionBy("level").orderBy(F.col("num_ks").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

unpacked = (
    lines
    .filter(F.col("k") == "trace")
    .select(
        "id",
        "service",
        "level",
        "k",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

metrics = (
    unpacked
    .select(
        "id",
        "service",
        "level",
        "k",
        "value",
        "has_fault",
        F.ntile(3).over(w).alias("status_code_high_bucket"),
        F.sum("has_fault").over(w2).alias("cumulative_has_fault"),
        F.ntile(5).over(w3).alias("has_fault_bucket"),
    )
)

cleaned = (
    metrics
    .filter(F.col("has_fault_bucket") == 7)
    .select(
        "id",
        "level",
        "k",
        "value",
        "has_fault",
        "status_code_high_bucket",
        "cumulative_has_fault",
        "has_fault_bucket",
        F.substring("level", 1, 3).alias("level_prefix"),
    )
)

result = (
    cleaned
    .groupBy(
        "level",
        F.when(F.col("status_code_high_bucket") >= 7, "LARGE").otherwise("SMALL").alias("status_code_high_bucket_tier"),
    )
    .agg(
        F.sum(F.when(F.col("level") == "INFO", F.col("has_fault_bucket")).otherwise(0)).alias("info_has_fault_bucket"),
        F.countDistinct("level").alias("distinct_levels"),
        F.size(F.collect_set("k")).alias("num_ks"),
    )
    .withColumn("num_ks_percent_rank", F.percent_rank().over(w4))
    .orderBy(F.col("status_code_high_bucket_tier").desc())
)
