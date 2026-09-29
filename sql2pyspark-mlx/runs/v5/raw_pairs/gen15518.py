from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "level",
        "latency_ms",
        "attr_key",
        "v",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

cleaned = (
    parsed
    .filter((~F.col("attr_key").eqNullSafe(F.lit("region"))) & (F.col("level") != "WARN"))
    .select("*", F.length("level").alias("level_length"))
)

valid = (
    cleaned
    .filter(F.col("attr_key").isNotNull() & (F.col("attr_key").eqNullSafe(F.lit("region"))))
    .withColumn("attr_key_lower", F.lower("attr_key"))
    .withColumn("level_length_num", F.col("level_length").cast("string"))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("DIRECT")))
)

result = (
    valid
    .select(
        "id",
        "level",
        "latency_ms",
        "attr_key",
        "flag_bit4",
        "attr_key_lower",
        "level_length_num",
        F.when(F.col("level").rlike("^(DEBUG|WARN)$"), "CORE").otherwise("GROUP_A").alias("level_zone"),
    )
    .orderBy("flag_bit4")
)
