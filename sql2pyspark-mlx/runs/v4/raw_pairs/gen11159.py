from pyspark.sql import functions as F

matched = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    matched
    .groupBy(F.date_trunc("WEEK", "starts_on").alias("week_start"))
    .agg(
        F.max(F.when(F.col("promo_code") == "BUNDLE", F.col("promo_id"))).alias("bundle_max_promo_id"),
        F.count_if(F.col("channel") == "store").alias("store_events"),
        F.percentile_approx("promo_id", [0.25, 0.5, 0.75]).alias("promo_id_quartiles"),
    )
    .orderBy("week_start")
)
