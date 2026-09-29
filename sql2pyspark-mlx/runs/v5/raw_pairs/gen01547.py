from pyspark.sql import functions as F

remaining_keys = (
    df_promotions
    .select("channel", "promo_code")
    .subtract(
        df_promotions
        .filter(F.col("promo_code").isin(["BUNDLE", "VIP", "SPRING10"]))
        .select("channel", "promo_code")
    )
)

kept_rows = (
    df_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("channel", "promo_code", "starts_on")
    .orderBy(F.col("promo_code").asc_nulls_first(), F.col("starts_on").desc())
)
