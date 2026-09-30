from pyspark.sql import functions as F

flattened = df_carts.select("id", "created_at", "coupon_codes", F.inline("items"))

matched_keys = (
    flattened
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.col("created_at") >= "2023-08-01")
        .select("id")
    )
)

reconciled = (
    flattened
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter((~F.col("promo").eqNullSafe(F.lit("SPRING10"))) & F.col("promo").isNotNull())
    .select(
        "*",
        F.col("product_id").cast("string").alias("product_id_str"),
        F.greatest("qty", "price").alias("greatest_qty_price"),
        F.coalesce("promo", F.lit("N/A")).alias("promo_clean"),
    )
)

cleaned = (
    valid
    .filter(F.col("created_at") >= F.lit("2025-03-01 21:00:00").cast("timestamp"))
    .select(
        "*",
        F.round((F.col("qty") / F.col("price")), 2).alias("qty_per_price"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("resolved_promo"),
    )
)

result = (
    cleaned
    .groupBy(
        "product_id",
        "promo",
        F.when(F.col("greatest_qty_price") >= 70, "HIGH").otherwise("LOW").alias("greatest_qty_price_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy("span_minutes")
)
