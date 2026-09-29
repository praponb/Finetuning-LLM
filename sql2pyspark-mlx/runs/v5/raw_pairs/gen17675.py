from pyspark.sql import functions as F

remaining_keys = (
    df_promotions
    .select("channel", "promo_code")
    .subtract(
        df_promotions
        .filter(F.col("discount_pct") > 0.31)
        .select("channel", "promo_code")
    )
)

reconciled = (
    df_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.date_trunc("YEAR", "ends_on").alias("year_start"),
        F.when(F.col("discount_pct") >= 0.36, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
        "channel",
    )
    .agg(
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.count("*").alias("row_count"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct_2"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("discount_pct_per_partner"),
    )
)
