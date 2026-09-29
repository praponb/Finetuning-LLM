from pyspark.sql import functions as F

kept_keys = (
    df_promotions
    .select("channel", "promo_code")
    .intersect(
        df_promotions
        .filter(F.col("promo_code").isin(["WELCOME5", "BUNDLE", "VIP"]))
        .select("channel", "promo_code")
    )
)

reconciled = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("week_start", F.date_trunc("week", "ends_on"))
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.34, "HIGH").otherwise("LOW"))
    .rollup("week_start", "discount_pct_tier", "channel")
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count(F.when(F.col("promo_code") == "WELCOME5", 1)).alias("welcome5_count"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.grouping("week_start").alias("gid"),
    )
)
