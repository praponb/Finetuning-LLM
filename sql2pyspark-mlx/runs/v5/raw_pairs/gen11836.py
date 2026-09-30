from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "log_ts",
        "k",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

matched_keys = (
    parsed
    .select("id")
    .intersectAll(
        df_app_logs
        .filter(F.to_date("log_ts") >= "2024-09-30")
        .select("id")
    )
)

verified = (
    parsed
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("k").isNotNull())
    .select(
        "k",
        "flag_bit1",
        "status_code_high",
        (F.col("flag_bit1") - F.col("status_code_high")).alias("flag_bit1_minus_status_code_high"),
        (F.col("flag_bit1") - F.col("status_code_high")).alias("flag_bit1_minus_status_code_high_2"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_filled"),
    )
)

result = (
    base
    .cube(
        F.when(F.col("flag_bit1") >= 1, "HIGH").otherwise("LOW").alias("flag_bit1_tier"),
        "k",
    )
    .agg(
        F.min("flag_bit1_minus_status_code_high_2").alias("min_flag_bit1_minus_status_code_high_2"),
        F.count_if(F.col("k") == "version").alias("version_events"),
        F.countDistinct("k").alias("distinct_ks"),
    )
    .orderBy(F.col("min_flag_bit1_minus_status_code_high_2").desc_nulls_last())
)
