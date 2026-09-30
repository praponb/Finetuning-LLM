from pyspark.sql import functions as F

matched_keys = (
    df_promotions
    .select("id", "channel")
    .exceptAll(
        df_promotions
        .filter(F.col("discount_pct") <= 0.29)
        .select("id", "channel")
    )
)

reconciled = (
    df_promotions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.36, "PREMIUM").otherwise("STANDARD"))
    .rollup("promo_code", "discount_pct_tier")
    .agg(
        F.max("discount_pct").alias("max_discount_pct"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "BUNDLE", 1)), F.lit(0)), 2).alias("discount_pct_per_bundle"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.count("*").alias("row_count"),
        F.max("discount_pct").alias("max_discount_pct_2"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("num_promo_codes") >= 1)
)
