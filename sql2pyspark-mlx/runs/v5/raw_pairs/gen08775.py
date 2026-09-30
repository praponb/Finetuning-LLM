from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select("id", "created_at", "coupon_codes", F.col("line.promo"))
)

base = (
    flattened
    .filter((F.col("promo").eqNullSafe(F.lit("BUNDLE"))) & F.col("promo").isNotNull())
    .select(
        "*",
        F.to_date("created_at").alias("created_at_date"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("promo_clean"),
    )
)

base_carts = (
    base
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & (F.year("created_at_date") == 2024))
    .select(
        "id",
        "created_at",
        "promo_clean",
        F.size("coupon_codes").alias("coupon_codes_count"),
    )
)

remaining_keys = (
    base_carts
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.col("created_at") < "2023-07-01")
        .select("id")
    )
)

verified = (
    base_carts
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = verified.select("promo_clean")
