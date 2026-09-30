from pyspark.sql import functions as F

remaining_keys = (
    df_promotions
    .select("channel")
    .exceptAll(
        df_promotions
        .filter(F.col("promo_code").isin(["BUNDLE", "VIP"]))
        .select("channel")
    )
)

reconciled = (
    df_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("WEEK", "ends_on").alias("week_start"))
    .agg(
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.count(F.when(F.col("promo_code") == "VIP", 1)).alias("vip_count"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
    )
)
