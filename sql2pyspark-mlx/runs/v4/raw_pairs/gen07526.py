from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

parsed = (
    lines
    .filter(F.col("level").isin(["DEBUG", "INFO"]))
    .select(
        "level",
        "attr_value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

filtered = (
    parsed
    .filter(F.col("status_code_high") != 2)
    .select("*", F.length("level").alias("level_length"))
)

result = (
    filtered
    .select("attr_value", "status_code_high", "level_length")
    .orderBy(F.col("status_code_high").asc_nulls_first(), "level_length")
)
