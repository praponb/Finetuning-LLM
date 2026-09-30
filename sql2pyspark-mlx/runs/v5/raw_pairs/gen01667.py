from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

unpacked = (
    flattened
    .filter(F.col("status_code") == 310)
    .select(
        "latency_ms",
        "status_code",
        "attr_key",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    unpacked
    .filter(F.col("attr_key").isin(["trace", "region"]))
    .select(
        "status_code",
        "attr_key",
        "has_fault",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
    )
)

filtered = (
    enriched
    .filter(F.col("status_code") > 263)
    .select(
        "*",
        F.coalesce("status_code_share", F.lit(0)).alias("status_code_share_filled"),
        F.coalesce("attr_key", F.lit("NONE")).alias("resolved_attr_key"),
    )
)

with_windows = (
    filtered
    .select(
        "status_code",
        "status_code_share",
        "status_code_share_filled",
        F.min("status_code").over(w).alias("attr_key_min_status_code"),
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share_2"),
    )
)

result = (
    with_windows
    .select(
        "status_code",
        "status_code_share",
        "status_code_share_filled",
        "attr_key_min_status_code",
        "status_code_share_2",
    )
)
