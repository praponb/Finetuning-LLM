from pyspark.sql import functions as F

remaining_keys = (
    df_promotions
    .select("channel", "promo_code")
    .subtract(
        df_promotions
        .filter(F.month("ends_on").isin([7, 9, 12]))
        .select("channel", "promo_code")
    )
)

verified = (
    df_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.27, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["discount_pct_tier", "channel"], ["discount_pct_tier"]],
        "discount_pct_tier",
        "channel",
    )
    .agg(
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.sum(F.when(F.col("promo_code") == "BUNDLE", F.col("discount_pct")).otherwise(0)).alias("bundle_discount_pct"),
        F.sum(F.when(F.col("promo_code") == "WELCOME5", F.col("discount_pct")).otherwise(0)).alias("welcome5_discount_pct"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
    )
    .filter(F.col("num_promo_codes") >= 1)
    .orderBy(F.col("welcome5_discount_pct").desc_nulls_first())
)
