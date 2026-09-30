from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

filtered = (
    exploded
    .filter(F.col("status_code") > 440)
    .withColumn("attr_key_host_key", F.concat_ws("/", "attr_key", "host"))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("N/A")))
)

cleaned = (
    filtered
    .filter(~F.col("level").isin(["WARN", "DEBUG"]))
    .withColumn("status_code_per_latency_ms", F.round((F.col("status_code") / F.col("latency_ms")), 2))
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "v",
        "attr_key_host_key",
        F.greatest("latency_ms", "status_code_per_latency_ms").alias("greatest_latency_ms_status_code_per_latency_ms"),
        F.initcap(F.trim("service")).alias("service_title"),
    )
)

by_type = (
    cleaned_app_logs
    .groupBy("level")
    .pivot("host", ["h3", "h4"])
    .agg(
        F.avg("greatest_latency_ms_status_code_per_latency_ms").alias("value_sum"),
        F.count(F.lit(1)).alias("n"),
    )
)

result = by_type.select("level", "h3_n", "h4_value_sum", "h4_n")
