from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("status_code_high", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy(F.col("status_code_high").desc(), "id", "attr_key")
w3 = Window.partitionBy("attr_key").orderBy("has_fault", "id", "attr_key").rowsBetween(-5, -1)
w4 = Window.partitionBy("level")
w5 = Window.partitionBy("attr_key").orderBy("level_max_status_code_high", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("attr_key").orderBy("has_fault", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

unpacked = (
    flattened
    .filter(F.col("level") != "INFO")
    .select(
        "id",
        "level",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
    )
)

enriched = (
    unpacked
    .select(
        "id",
        "attr_key",
        "has_fault",
        F.first("has_fault", ignorenulls=True).over(w).alias("first_known_has_fault"),
        F.row_number().over(w2).alias("seq_no"),
        F.avg("status_code_high").over(w3).alias("status_code_high_avg_5_prior"),
        F.max("status_code_high").over(w4).alias("level_max_status_code_high"),
    )
)

cleaned = (
    enriched
    .filter(F.col("attr_key").isNotNull() & F.col("status_code_high_avg_5_prior").isNull())
    .withColumn("status_code_high_avg_5_prior_band", F.when(F.col("status_code_high_avg_5_prior") >= 3, "High").when(F.col("status_code_high_avg_5_prior") >= 1, "Medium").otherwise("Low"))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("DIRECT")))
)

with_windows = (
    cleaned
    .filter(F.col("first_known_has_fault").isNotNull())
    .withColumn("running_min_level_max_status_code_high", F.min("level_max_status_code_high").over(w5))
    .withColumn("running_sum_level_max_status_code_high", F.sum("level_max_status_code_high").over(w5))
    .withColumn("running_avg_level_max_status_code_high", F.avg("level_max_status_code_high").over(w5))
    .withColumn("last_known_first_known_has_fault", F.last("first_known_has_fault", ignorenulls=True).over(w6))
)

result = (
    with_windows
    .select(
        "id",
        "attr_key",
        "has_fault",
        "status_code_high_avg_5_prior",
        "level_max_status_code_high",
        "attr_key_filled",
        "running_sum_level_max_status_code_high",
        "running_avg_level_max_status_code_high",
        "last_known_first_known_has_fault",
    )
    .orderBy(F.col("level_max_status_code_high").desc(), "has_fault")
)
