from pyspark.sql import functions as F

kept_keys = (
    df_promotions
    .select("id", "promo_code")
    .exceptAll(
        df_promotions
        .filter(F.col("promo_code").isin(["SPRING10", "WELCOME5"]))
        .select("id", "promo_code")
    )
)

kept_rows = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("discount_pct") >= 0.35, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
        F.date_trunc("WEEK", "starts_on").alias("week_start"),
    )
    .agg(
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list_2"),
    )
)
