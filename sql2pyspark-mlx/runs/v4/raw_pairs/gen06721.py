from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

parsed = (
    flattened
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        "k",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
    )
)

base = (
    parsed
    .filter(F.col("status_code") == 374)
    .select(
        "id",
        "service",
        "host",
        "k",
        "status_code_high",
        F.col("status_code").cast("double").alias("status_code_str"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_filled"),
    )
)

result = base.select("id", "k", "status_code_high", "status_code_str", "k_filled")
