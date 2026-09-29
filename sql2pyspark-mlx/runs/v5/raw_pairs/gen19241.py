from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("promotion_discount_pct").desc())
w2 = Window.partitionBy("promotion_promo_code")
w3 = Window.orderBy("promotion_discount_pct_cume_dist")
w4 = Window.partitionBy("channel", "promotion_promo_code")
w5 = Window.orderBy(F.col("promotion_discount_pct_cume_dist_cume_dist").desc_nulls_first())
w6 = Window.orderBy(F.col("promotion_discount_pct_cume_dist_cume_dist").desc())
w7 = Window.partitionBy("promo_code")

with_window = (
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

kept_keys = (
    with_window
    .select("channel", "promo_code")
    .subtract(
        df_promotions
        .filter(F.month("starts_on").isin([8, 9, 11]))
        .select("channel", "promo_code")
    )
)

reconciled = (
    with_window
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "id",
        "channel",
        "promo_code",
        "promotion_promo_code",
        F.cume_dist().over(w).alias("promotion_discount_pct_cume_dist"),
    )
)

scored_promotions = (
    scored
    .select(
        "channel",
        "promo_code",
        "promotion_promo_code",
        "promotion_discount_pct_cume_dist",
        F.stddev_samp("promotion_discount_pct_cume_dist").over(w2).alias("promotion_promo_code_stddev_samp_promotion_discount_pct_cume_dist"),
        F.cume_dist().over(w3).alias("promotion_discount_pct_cume_dist_cume_dist"),
        F.min("promotion_discount_pct_cume_dist").over(w4).alias("channel_min_promotion_discount_pct_cume_dist"),
    )
)

enriched = (
    scored_promotions
    .select(
        "*",
        F.percent_rank().over(w5).alias("promotion_discount_pct_cume_dist_cume_dist_percent_rank"),
        F.dense_rank().over(w6).alias("promotion_discount_pct_cume_dist_cume_dist_dense_rank"),
        F.max("promotion_discount_pct_cume_dist_cume_dist").over(w7).alias("promo_code_max_promotion_discount_pct_cume_dist_cume_dist"),
    )
)

filtered = (
    enriched
    .filter(F.col("promotion_promo_code").isNotNull() & (F.col("promotion_discount_pct_cume_dist_cume_dist") > 0.37))
    .select(
        "promo_code",
        "promotion_discount_pct_cume_dist",
        "promotion_promo_code_stddev_samp_promotion_discount_pct_cume_dist",
        "channel_min_promotion_discount_pct_cume_dist",
        "promotion_discount_pct_cume_dist_cume_dist_percent_rank",
        "promotion_discount_pct_cume_dist_cume_dist_dense_rank",
        F.length("promotion_promo_code").alias("promotion_promo_code_length"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

summary = (
    filtered
    .groupBy(
        F.when(F.col("promotion_promo_code_length") >= 68, "PREMIUM").otherwise("STANDARD").alias("promotion_promo_code_length_tier"),
        F.when(F.col("channel_min_promotion_discount_pct_cume_dist") >= 1, "HIGH").otherwise("LOW").alias("channel_min_promotion_discount_pct_cume_dist_tier"),
    )
    .agg(
        F.count(F.when(F.col("promo_code") == "BUNDLE", 1)).alias("bundle_rows"),
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_count"),
        F.sum(F.when(F.col("promo_code") == "BUNDLE", F.col("promotion_discount_pct_cume_dist_cume_dist_dense_rank"))).alias("bundle_total_promotion_discount_pct_cume_dist_cume_dist_dense_rank"),
        F.sum(F.when(F.col("promo_code") == "WELCOME5", F.col("promotion_discount_pct_cume_dist_cume_dist_dense_rank")).otherwise(0)).alias("welcome5_promotion_discount_pct_cume_dist_cume_dist_dense_rank"),
    )
    .filter(F.col("bundle_total_promotion_discount_pct_cume_dist_cume_dist_dense_rank") > 0)
)

result = (
    summary
    .select(
        "promotion_promo_code_length_tier",
        "channel_min_promotion_discount_pct_cume_dist_tier",
        "bundle_rows",
        "spring10_count",
        "bundle_total_promotion_discount_pct_cume_dist_cume_dist_dense_rank",
        "welcome5_promotion_discount_pct_cume_dist_cume_dist_dense_rank",
    )
    .orderBy(F.col("spring10_count").asc_nulls_last())
)
