from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

flattened_attrs = (
    lines
    .select(
        "service",
        "level",
        "latency_ms",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
    )
)

base = (
    flattened_attrs
    .filter(F.col("level") == "DEBUG")
    .select(
        "service",
        "latency_ms",
        "v",
        "status_code_high",
        "is_flagged",
        (F.col("latency_ms") * F.col("is_flagged")).alias("latency_ms_times_is_flagged"),
    )
)

grouped = (
    base
    .withColumn("is_flagged_tier", F.when(F.col("is_flagged") >= 1, "HIGH").otherwise("LOW"))
    .withColumn("status_code_high_tier", F.when(F.col("status_code_high") >= 2, "PREMIUM").otherwise("STANDARD"))
    .rollup("is_flagged_tier", "status_code_high_tier")
    .agg(
        F.max(F.when(F.col("service") == "checkout", F.col("latency_ms"))).alias("checkout_max_latency_ms"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.grouping("is_flagged_tier").alias("gid"),
    )
    .filter(F.col("num_services") < 1)
)

result = (
    grouped
    .select(
        "is_flagged_tier",
        "status_code_high_tier",
        "checkout_max_latency_ms",
        "service_list",
        "num_services",
        "gid",
    )
    .orderBy(F.col("status_code_high_tier").desc())
)
