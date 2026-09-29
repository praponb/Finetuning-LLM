from pyspark.sql import functions as F

kept_keys = (
    df_promotions
    .select("promo_code")
    .intersect(
        df_promotions
        .filter(F.year("ends_on") >= 2024)
        .select("promo_code")
    )
)

verified = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "promo_code",
        "discount_pct",
        F.when(F.col("promo_code").rlike("^(BUNDLE|WELCOME5)$"), "PRIORITY").when(F.col("promo_code").rlike("^(SPRING10|VIP)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("promo_code_zone"),
    )
    .orderBy("promo_code")
)
