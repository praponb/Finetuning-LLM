from pyspark.sql import functions as F

with_window = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    with_window
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.33, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["discount_pct_tier", "channel"], ["discount_pct_tier"], []],
        "discount_pct_tier",
        "channel",
    )
    .agg(
        F.percentile_approx("promotion_discount_pct", 0.95).alias("promotion_discount_pct_p95"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("promotion_discount_pct_p95") > 0)
    .orderBy("promotion_discount_pct_p95")
)
