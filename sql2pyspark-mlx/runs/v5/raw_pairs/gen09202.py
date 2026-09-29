from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("promo_code")
w_hist = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w4 = Window.partitionBy("promotion_promo_code", "promo_code").orderBy(F.col("promotion_discount_pct").asc_nulls_first())
w5 = Window.partitionBy("promotion_promo_code", "promo_code").orderBy(F.col("is_spring10").desc())
w6 = Window.partitionBy("promo_code").orderBy(F.col("promotion_discount_pct_dense_rank").desc())
w7 = Window.partitionBy("promotion_promo_code")
w8 = Window.orderBy(F.col("channel_min_discount_pct").desc_nulls_last())
w9 = Window.partitionBy("promo_code", "promotion_promo_code")

valid = (
    df_promotions
    .filter(F.col("promo_code").isin(["SPRING10", "WELCOME5"]))
    .withColumn("promo_code_upper", F.upper("promo_code"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("NONE")))
)

kept_keys = (
    valid
    .select("id", "channel")
    .subtract(
        df_promotions
        .filter(F.col("discount_pct") > 0.23)
        .select("id", "channel")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

with_window = (
    verified
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & F.col("s.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

scored = (
    with_window
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "promo_code_upper",
        "resolved_channel",
        "promotion_promo_code",
        "promotion_discount_pct",
        F.min("discount_pct").over(w).alias("channel_min_discount_pct"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share_2"),
        F.dense_rank().over(w_hist).alias("discount_pct_dense_rank"),
        F.dense_rank().over(w4).alias("promotion_discount_pct_dense_rank"),
    )
)

filtered = (
    scored
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-06-01"))) & (F.col("channel_min_discount_pct") == 0))
    .select(
        "promo_code",
        "starts_on",
        "ends_on",
        "resolved_channel",
        "promotion_promo_code",
        "promotion_discount_pct",
        "channel_min_discount_pct",
        "discount_pct_share",
        "discount_pct_share_2",
        "promotion_discount_pct_dense_rank",
        F.when(F.col("promo_code") == "SPRING10", 1).otherwise(0).alias("is_spring10"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.rank().over(w5).alias("is_spring10_rank"),
        F.dense_rank().over(w6).alias("promotion_discount_pct_dense_rank_dense_rank"),
        F.min("is_spring10").over(w7).alias("promotion_promo_code_min_is_spring10"),
        F.cume_dist().over(w8).alias("channel_min_discount_pct_cume_dist"),
        F.sum("discount_pct_share_2").over(w9).alias("promo_code_sum_discount_pct_share_2"),
    )
)

valid_promotions = (
    enriched
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-08-31"))) & (F.col("promotion_promo_code").eqNullSafe(F.lit("BUNDLE"))))
    .select(
        "promo_code",
        "starts_on",
        "ends_on",
        "promotion_promo_code",
        "promotion_discount_pct",
        "channel_min_discount_pct",
        "discount_pct_share_2",
        "promotion_discount_pct_dense_rank",
        "is_spring10_rank",
        "promotion_discount_pct_dense_rank_dense_rank",
        "promotion_promo_code_min_is_spring10",
        "promo_code_sum_discount_pct_share_2",
        F.when(F.col("channel_min_discount_pct_cume_dist") >= 0.76, "High").when(F.col("channel_min_discount_pct_cume_dist") >= 0.22, "Medium").otherwise("Low").alias("channel_min_discount_pct_cume_dist_band"),
    )
)

result = (
    valid_promotions
    .select(
        "promo_code",
        "starts_on",
        "ends_on",
        "promotion_promo_code",
        "channel_min_discount_pct",
        "discount_pct_share_2",
        "promotion_discount_pct_dense_rank",
        "is_spring10_rank",
        "channel_min_discount_pct_cume_dist_band",
    )
)
