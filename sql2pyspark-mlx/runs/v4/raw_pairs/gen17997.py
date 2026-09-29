from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

flattened_attrs = (
    lines
    .filter(F.col("k").isin(["trace", "region"]))
    .select(
        "id",
        "log_ts",
        "k",
        "value",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = (
    flattened_attrs
    .withColumn("k_min_has_fault", F.min("has_fault").over(w))
    .withColumn("has_fault_count_1h", F.count("has_fault").over(w2))
    .withColumn("status_code_high_sum_30m", F.sum("status_code_high").over(w3))
    .withColumn("k_count_has_fault", F.count("has_fault").over(w))
)

result = (
    metrics
    .select(
        "id",
        "log_ts",
        "k",
        "has_fault",
        "k_min_has_fault",
        "status_code_high_sum_30m",
        "k_count_has_fault",
        F.when(F.col("k").rlike("^(trace|region)$"), "EXTENDED").when(F.col("k").rlike("^(version)$"), "GROUP_B").otherwise("CORE").alias("k_zone"),
    )
    .orderBy(F.col("log_ts").desc_nulls_last())
)
