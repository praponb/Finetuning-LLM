from pyspark.sql import functions as F

lines = df_carts.select("id", "coupon_codes", F.explode_outer("attributes").alias("k", "v"))

kept_keys = (
    lines
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.array_contains("coupon_codes", "BUNDLE"))
        .select("id")
    )
)

reconciled = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter((~F.col("k").eqNullSafe(F.lit("gift"))) & F.array_contains("coupon_codes", "BUNDLE"))
    .withColumn("coupon_codes_count", F.size("coupon_codes"))
)

base_carts = (
    base
    .filter(F.array_contains("coupon_codes", "WELCOME5"))
    .select(
        "coupon_codes",
        "k",
        "v",
        "coupon_codes_count",
        F.col("coupon_codes_count").cast("string").alias("coupon_codes_count_str"),
        F.round("coupon_codes_count", 0).alias("coupon_codes_count_rounded"),
    )
)

valid = (
    base_carts
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        F.round("coupon_codes_count_rounded", 0).alias("coupon_codes_count_rounded_rounded"),
        F.when(F.col("k").isin(["gift", "weight_kg"]), "Tier 1").otherwise("Other").alias("k_group"),
    )
)

result = (
    valid
    .select(
        "k",
        F.col("coupon_codes_count_rounded_rounded").alias("COUPON_CODES_COUNT_ROUNDED_ROUNDED_VAL"),
        F.col("coupon_codes_count_rounded").alias("COUPON_CODES_COUNT_ROUNDED"),
    )
    .unpivot(
        ids=["k"],
        values=["COUPON_CODES_COUNT_ROUNDED_ROUNDED_VAL", "COUPON_CODES_COUNT_ROUNDED"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
