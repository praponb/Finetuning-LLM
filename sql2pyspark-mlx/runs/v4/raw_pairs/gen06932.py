from pyspark.sql import functions as F

attributed = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & F.col("e.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    attributed
    .rollup("channel")
    .agg(
        F.count_if(F.col("promotion_promo_code") == "SPRING10").alias("spring10_events"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.round(F.avg("promo_id"), 2).alias("avg_promo_id"),
        F.min("promo_id").alias("min_promo_id"),
        F.percentile_approx("promotion_discount_pct", [0.25, 0.5, 0.75]).alias("promotion_discount_pct_quartiles"),
        F.percentile_approx("promotion_discount_pct", 0.5).alias("promotion_discount_pct_p50"),
    )
    .orderBy(F.col("spring10_events").desc_nulls_first())
)
