from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

flattened_attrs = (
    flattened
    .select(
        "id",
        "attr_key",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    flattened_attrs
    .filter(F.col("attr_key").isNotNull() & (F.col("status_code_high") <= 2))
    .select(
        "*",
        F.greatest("status_code_high", "flag_bit2").alias("greatest_status_code_high_flag_bit2"),
    )
)

filtered = (
    valid
    .filter(F.col("greatest_status_code_high_flag_bit2") < 35)
    .withColumn("attr_key_lower", F.lower("attr_key"))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

result = (
    filtered
    .select(
        "id",
        "attr_key",
        "flag_bit2",
        "status_code_high",
        "greatest_status_code_high_flag_bit2",
        "attr_key_lower",
        "attr_key_filled",
        F.when(F.col("attr_key").rlike("^(trace|region)$"), "GROUP_A").otherwise("PRIORITY").alias("attr_key_zone"),
    )
    .orderBy(F.col("status_code_high").desc(), F.col("attr_key").desc())
)
