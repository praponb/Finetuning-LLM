from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w_grp = Window.orderBy(F.col("status_code").desc())
w_part = Window.partitionBy("host", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

base = (
    lines
    .filter(F.col("host").isin(["h2", "h4"]))
    .select(
        "*",
        F.lower("key").alias("key_lower"),
        F.lower("level").alias("level_lower"),
        F.coalesce("key", F.lit("DIRECT")).alias("key_filled"),
    )
)

cleaned = (
    base
    .filter(F.col("host") != "h3")
    .select(
        "*",
        (F.col("latency_ms") * F.col("status_code")).alias("latency_ms_times_status_code"),
        F.concat("host", F.lit(" - "), "key").alias("host_key"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.trim("host").alias("clean_host"),
        F.substring("host", 1, 2).alias("host_prefix"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_filled_2"),
    )
)

filtered = (
    cleaned_app_logs
    .filter(~F.col("key").eqNullSafe(F.lit("trace")))
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "key",
        "key_lower",
        "level_lower",
        "key_filled",
        "host_key",
        "host_prefix",
        "key_filled_2",
        F.coalesce("key", F.lit("Unknown")).alias("key_filled_3"),
    )
)

base_app_logs = (
    filtered
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("region"))))
    .select(
        "*",
        F.col("latency_ms").cast("string").alias("latency_ms_str"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("resolved_key"),
    )
)

with_windows = (
    base_app_logs
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w))
    .withColumn("status_code_rank", F.rank().over(w_grp))
    .withColumn("corr_status_code_latency_ms", F.corr("status_code", "latency_ms").over(w_part))
)

result = (
    with_windows
    .select(
        "id",
        "level",
        "status_code",
        "key",
        "key_lower",
        "level_lower",
        "key_filled",
        "host_key",
        "key_filled_2",
        F.when(F.col("key").rlike("^(trace|version)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("key_zone"),
    )
    .orderBy(F.col("key").desc_nulls_last())
)
