from pyspark.sql import functions as F

flattened = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("code"))

valid = (
    flattened
    .filter(F.col("code").isNotNull())
    .select("*", F.trim("code").alias("clean_code"))
)

filtered = (
    valid
    .filter(F.col("created_at") >= F.to_timestamp(F.lit("2025-03-02 03:00:00")))
    .withColumn("code_filled", F.coalesce("code", F.lit("unassigned")))
    .withColumn("resolved_code", F.coalesce("code", F.lit("N/A")))
)

valid_carts = (
    filtered
    .filter((F.col("created_at") < "2025-02-01") & (~F.col("code").eqNullSafe(F.lit("WELCOME5"))))
    .select(
        "id",
        "created_at",
        "code",
        "clean_code",
        "code_filled",
        "resolved_code",
        F.dayofmonth("created_at").alias("created_at_day"),
        F.coalesce("code", F.lit("N/A")).alias("resolved_code_2"),
    )
)

result = (
    valid_carts
    .rollup(
        F.when(F.col("created_at_day") >= 69, "PREMIUM").otherwise("STANDARD").alias("created_at_day_tier"),
    )
    .agg(
        F.count_if(F.col("code") == "BUNDLE").alias("bundle_events"),
        F.count("*").alias("num_rows"),
        F.round(F.sum("created_at_day") / F.nullif(F.count(F.when(F.col("code") == "WELCOME5", 1)), F.lit(0)), 2).alias("created_at_day_per_welcome5"),
    )
)
