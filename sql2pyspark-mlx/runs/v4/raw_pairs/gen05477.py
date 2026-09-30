from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("ends_on") >= "2023-08-01"))
    .select(
        "*",
        F.year("ends_on").alias("ends_on_year"),
        F.add_months("starts_on", 12).alias("starts_on_plus_months"),
    )
)

filtered = (
    cleaned
    .filter(F.col("channel").isNotNull() & (F.col("ends_on") <= "2025-06-01"))
    .select("*", F.round("ends_on_year", 2).alias("ends_on_year_rounded"))
)

matched = (
    filtered
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

kept_keys = (
    matched
    .select("id", "promo_code")
    .intersect(
        df_promotions
        .filter(F.col("starts_on").between("2025-03-02", "2025-08-25"))
        .select("id", "promo_code")
    )
)

kept_rows = (
    matched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "channel", "starts_on_plus_months", "ends_on_year_rounded")
    .orderBy(F.col("id").asc_nulls_first())
)
