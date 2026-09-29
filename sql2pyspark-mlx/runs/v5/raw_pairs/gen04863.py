from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("has_fault_tier").orderBy(F.col("region_adjusted_has_fault").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

flattened_attrs = (
    flattened
    .select(
        "status_code",
        "k",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
    )
)

filtered = (
    flattened_attrs
    .filter(F.col("status_code") == 319)
    .select(
        "k",
        "attr_value",
        "status_code_high",
        "has_fault",
        (F.col("has_fault") / F.nullif("status_code_high", F.lit(0))).alias("has_fault_per_status_code_high"),
    )
)

filtered_app_logs = (
    filtered
    .filter(F.col("has_fault_per_status_code_high").between(27, 54))
    .select(
        "*",
        F.when(F.col("k").isin(["region", "trace"]), "Group A").otherwise("Other").alias("k_group"),
        F.coalesce("k", F.lit("N/A")).alias("k_clean"),
    )
)

valid = (
    filtered_app_logs
    .filter(F.col("has_fault") <= 0)
    .withColumn("adjusted_has_fault", F.round((F.col("has_fault") * 1.1), 2))
)

result = (
    valid
    .groupBy(
        F.when(F.col("has_fault") >= 1, "PREMIUM").otherwise("STANDARD").alias("has_fault_tier"),
        "k",
    )
    .agg(
        F.sum(F.when(F.col("k") == "region", F.col("adjusted_has_fault")).otherwise(0)).alias("region_adjusted_has_fault"),
        F.sum("has_fault").alias("total_has_fault"),
    )
    .withColumn("region_adjusted_has_fault_percent_rank", F.percent_rank().over(w))
)
