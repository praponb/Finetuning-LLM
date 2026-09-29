from pyspark.sql import functions as F

matched_keys = (
    df_promotions
    .select("channel")
    .intersectAll(
        df_promotions
        .filter(F.col("promo_code") == "WELCOME5")
        .select("channel")
    )
)

kept_rows = (
    df_promotions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.when(F.col("promo_code").rlike("^(VIP|BUNDLE)$"), "OTHER_ZONE").otherwise("CORE").alias("promo_code_zone"),
    )
    .orderBy(F.col("promo_code").asc_nulls_last())
)
