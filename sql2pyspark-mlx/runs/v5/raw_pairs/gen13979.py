from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("k", "host")
w3 = Window.partitionBy("k").orderBy(F.col("status_code_high").desc_nulls_last())

lines = (
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
    lines
    .filter(F.col("host").isin(["h3", "h2"]))
    .select(
        "id",
        "host",
        "log_ts",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
    )
)

remaining_keys = (
    parsed
    .select("host")
    .exceptAll(
        df_app_logs
        .filter(F.col("log_ts") >= "2025-09-01")
        .select("host")
    )
)

kept_rows = (
    parsed
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 11:00:00")))
    .withColumn("status_code_high_times_is_flagged", (F.col("status_code_high") * F.col("is_flagged")))
)

with_windows = (
    filtered
    .filter(F.col("status_code_high_times_is_flagged") <= 30)
    .select(
        "*",
        F.max("status_code_high").over(w).alias("host_max_status_code_high"),
        F.min("status_code_high").over(w2).alias("k_min_status_code_high"),
        F.cume_dist().over(w3).alias("status_code_high_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "k",
        "status_code_high",
        "is_flagged",
        "status_code_high_times_is_flagged",
        "host_max_status_code_high",
        F.when(F.col("k").rlike("^(region|version)$"), "GROUP_B").when(F.col("k").rlike("^(trace)$"), "OTHER_ZONE").otherwise("CORE").alias("k_zone"),
    )
    .orderBy(F.col("is_flagged").desc_nulls_last())
)
