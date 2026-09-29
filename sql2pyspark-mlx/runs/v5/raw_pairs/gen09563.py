from pyspark.sql import functions as F

kept_keys = (
    df_promotions
    .select("promo_code", "channel")
    .subtract(
        df_promotions
        .filter(F.year("ends_on") == 2025)
        .select("promo_code", "channel")
    )
)

verified = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.when(F.col("promo_code").rlike("^(BUNDLE|SPRING10)$"), "CORE").otherwise("PRIORITY").alias("promo_code_zone"),
    )
    .orderBy(F.col("discount_pct").desc())
)
