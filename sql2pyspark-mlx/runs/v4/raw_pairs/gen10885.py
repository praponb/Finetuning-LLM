from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        "k",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

remaining_keys = (
    parsed
    .select("id")
    .intersectAll(
        df_app_logs
        .filter(F.col("level") == "INFO")
        .select("id")
    )
)

kept_rows = (
    parsed
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "host",
        "status_code",
        "k",
        "is_flagged",
        "status_code_high",
        F.when(F.col("k").rlike("^(region|trace)$"), "GROUP_A").when(F.col("k").rlike("^(version)$"), "GROUP_B").otherwise("PRIORITY").alias("k_zone"),
    )
    .orderBy(F.col("is_flagged").asc_nulls_last(), "k_zone")
)
