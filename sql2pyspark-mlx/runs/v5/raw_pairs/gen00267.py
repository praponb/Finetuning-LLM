from pyspark.sql import functions as F

kept_keys = (
    df_promotions
    .select("promo_code", "id")
    .intersectAll(
        df_promotions
        .filter(F.col("channel").isin(["store", "web"]))
        .select("promo_code", "id")
    )
)

reconciled = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("channel", "promo_code", "starts_on", "ends_on", "discount_pct")
    .orderBy(F.col("channel").desc_nulls_first())
)
