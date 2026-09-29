from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

unpacked = (
    lines
    .select(
        "latency_ms",
        "status_code",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

metrics = (
    unpacked
    .select(
        "*",
        F.corr("status_code", "latency_ms").over(w_ordered).alias("corr_status_code_latency_ms"),
        F.max("status_code_high").over(w_ordered).alias("key_max_status_code_high"),
    )
)

cleaned = (
    metrics
    .filter(F.col("has_fault") < 0)
    .select("*", F.initcap(F.trim("key")).alias("key_title"))
)

result = (
    cleaned
    .select(
        "latency_ms",
        "status_code",
        "status_code_high",
        "has_fault",
        "corr_status_code_latency_ms",
        "key_max_status_code_high",
        "key_title",
    )
    .orderBy(
        F.col("latency_ms").asc_nulls_first(),
        F.col("corr_status_code_latency_ms").desc(),
    )
)
