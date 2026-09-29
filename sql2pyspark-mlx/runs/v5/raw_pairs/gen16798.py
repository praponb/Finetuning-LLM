from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "service", "status_code", F.explode("context").alias("k", "v"))
)

unpacked = (
    lines
    .select(
        "id",
        "status_code",
        "k",
        "v",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    unpacked
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        (F.col("status_code") / F.nullif("flag_bit8", F.lit(0))).alias("status_code_per_flag_bit8"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_clean"),
    )
)

kept_keys = (
    valid
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.to_date("log_ts") >= "2025-10-05")
        .select("id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("status_code_per_flag_bit8_tier", F.when(F.col("status_code_per_flag_bit8") >= 66, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["k", "status_code_per_flag_bit8_tier"], ["k"]],
        "k",
        "status_code_per_flag_bit8_tier",
    )
    .agg(
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.count(F.when(F.col("k") == "region", 1)).alias("region_rows"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("k")
)
