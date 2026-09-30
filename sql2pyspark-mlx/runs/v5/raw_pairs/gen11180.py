from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

parsed = (
    flattened
    .filter(F.col("status_code") < 437)
    .select(
        "id",
        "latency_ms",
        "status_code",
        "value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

cleaned = (
    parsed
    .filter(F.col("flag_bit8") < 1)
    .select(
        "id",
        "latency_ms",
        "status_code",
        "flag_bit8",
        "status_code_high",
        (F.col("status_code") / F.nullif("flag_bit8", F.lit(0))).alias("status_code_per_flag_bit8"),
        F.col("status_code_high").cast("double").alias("status_code_high_num"),
    )
)

valid = (
    cleaned
    .filter(F.col("status_code_high") >= 2)
    .select(
        "id",
        "latency_ms",
        "status_code",
        "status_code_high",
        "status_code_per_flag_bit8",
        "status_code_high_num",
        F.ceil("latency_ms").alias("latency_ms_whole"),
        F.least("status_code", "status_code_per_flag_bit8").alias("least_status_code_status_code_per_flag_bit8"),
    )
)

valid_app_logs = (
    valid
    .filter(F.col("latency_ms") == 1232)
    .select(
        "latency_ms",
        "status_code",
        "status_code_per_flag_bit8",
        "status_code_high_num",
        "latency_ms_whole",
        "least_status_code_status_code_per_flag_bit8",
        (F.col("status_code_high") - F.col("latency_ms")).alias("status_code_high_minus_latency_ms"),
    )
)

base = (
    valid_app_logs
    .filter(F.col("latency_ms").between(1291, 1879))
    .select(
        "*",
        F.round((F.col("latency_ms") * 100), 2).alias("latency_ms_adjusted"),
        (F.col("least_status_code_status_code_per_flag_bit8") + F.col("latency_ms_whole")).alias("least_status_code_status_code_per_flag_bit8_plus_latency_ms_whole"),
    )
)

cleaned_app_logs = (
    base
    .filter(F.col("least_status_code_status_code_per_flag_bit8_plus_latency_ms_whole") >= 79)
    .select(
        "status_code",
        "status_code_per_flag_bit8",
        "status_code_high_num",
        "least_status_code_status_code_per_flag_bit8",
        "status_code_high_minus_latency_ms",
        "latency_ms_adjusted",
        "least_status_code_status_code_per_flag_bit8_plus_latency_ms_whole",
        F.ceil("latency_ms_whole").alias("latency_ms_whole_whole"),
        (F.col("latency_ms") * F.col("status_code_high_num")).alias("latency_ms_times_status_code_high_num"),
    )
)

stats = (
    cleaned_app_logs
    .groupBy(
        F.when(F.col("latency_ms_adjusted") >= 65, "HIGH").otherwise("LOW").alias("latency_ms_adjusted_tier"),
        F.when(F.col("status_code_high_num") >= 74, "LARGE").otherwise("SMALL").alias("status_code_high_num_tier"),
    )
    .agg(
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.percentile_approx("latency_ms_times_status_code_high_num", [0.25, 0.5, 0.75]).alias("latency_ms_times_status_code_high_num_quartiles"),
    )
)

result = (
    stats
    .select(
        "latency_ms_adjusted_tier",
        "status_code_high_num_tier",
        "status_code_bit_or",
        "latency_ms_times_status_code_high_num_quartiles",
    )
    .orderBy("status_code_bit_or")
)
