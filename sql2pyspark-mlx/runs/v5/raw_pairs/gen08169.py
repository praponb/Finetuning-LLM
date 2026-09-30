from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "latency_ms",
        "status_code",
        "attr_value",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

filtered = (
    flattened_attrs
    .filter(F.col("flag_bit4") <= 0)
    .select("*", F.round((F.col("flag_bit4") * 0.9), 2).alias("flag_bit4_adjusted"))
)

filtered_app_logs = (
    filtered
    .filter(F.col("latency_ms").between(1040, 1648))
    .withColumn("status_code_str", F.col("status_code").cast("double"))
    .withColumn("flag_bit4_times_status_code_high", (F.col("flag_bit4") * F.col("status_code_high")))
)

result = (
    filtered_app_logs
    .cube(
        F.when(F.col("status_code_str") >= 79, "HIGH").otherwise("LOW").alias("status_code_str_tier"),
        F.when(F.col("flag_bit4") >= 1, "PREMIUM").otherwise("STANDARD").alias("flag_bit4_tier"),
        F.when(F.col("flag_bit4_times_status_code_high") >= 80, "HIGH").otherwise("LOW").alias("flag_bit4_times_status_code_high_tier"),
    )
    .agg(
        F.sum("flag_bit4_adjusted").alias("total_flag_bit4_adjusted"),
        F.round(F.avg("flag_bit4"), 2).alias("avg_flag_bit4"),
        F.sum("status_code_str").alias("total_status_code_str"),
        F.min("status_code").alias("min_status_code"),
        F.percentile_approx("flag_bit4_times_status_code_high", 0.9).alias("flag_bit4_times_status_code_high_p90"),
        F.percentile_approx("flag_bit4_adjusted", [0.25, 0.5, 0.75]).alias("flag_bit4_adjusted_quartiles"),
    )
)
