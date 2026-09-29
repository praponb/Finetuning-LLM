from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w3 = Window.partitionBy("promo_code").orderBy("starts_on", "id", "promo_id")
w4 = Window.partitionBy("channel").orderBy(F.col("discount_pct_avg_6_prior").desc(), "id", "promo_id")
w5 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id", "promo_id")
w6 = Window.partitionBy("promo_code")
w7 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_avg_6_prior").desc_nulls_last())
w8 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct_avg_6_prior").desc_nulls_first())
w9 = Window.partitionBy("channel").orderBy(F.col("promo_id").asc_nulls_first())

with_windows = (
    df_promotions
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        F.avg("discount_pct").over(w).alias("discount_pct_avg_6_prior"),
        F.ntile(3).over(w2).alias("discount_pct_bucket"),
    )
)

matched = (
    with_windows
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

ranked = (
    matched
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        "discount_pct_avg_6_prior",
        "promo_id",
        "promo_code",
        "promotion_discount_pct",
    )
)

deduped = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

deduped_promotions = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

matched_keys = (
    deduped_promotions
    .select("id")
    .exceptAll(
        df_promotions
        .filter(F.col("discount_pct").between(0.2, 0.26))
        .select("id")
    )
)

reconciled = (
    deduped_promotions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w6)), 4))
    .withColumn("discount_pct_avg_6_prior_rank", F.rank().over(w7))
    .withColumn("discount_pct_avg_6_prior_percent_rank", F.percent_rank().over(w8))
    .withColumn("promo_id_cume_dist", F.cume_dist().over(w9))
)

grouped = (
    scored
    .withColumn("promo_id_cume_dist_tier", F.when(F.col("promo_id_cume_dist") >= 0.76, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["promo_id_cume_dist_tier", "promo_id"], ["promo_id_cume_dist_tier"], []],
        "promo_id_cume_dist_tier",
        "promo_id",
    )
    .agg(
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("discount_pct", 0.95).alias("discount_pct_p95"),
        F.grouping("promo_id_cume_dist_tier").alias("gid"),
    )
)

result = (
    grouped
    .select("partner_events", "discount_pct_p95")
    .orderBy(F.col("discount_pct_p95").desc())
)
