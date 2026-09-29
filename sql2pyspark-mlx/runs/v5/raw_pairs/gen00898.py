from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w_ordered = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")
w_recent = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w4 = Window.partitionBy("promo_code")
w5 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_bucket").asc_nulls_last())
w6 = Window.partitionBy("promo_code").orderBy("row_seq")

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull() & F.col("channel").isin(["web", "store", "mobile"]))
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.initcap(F.trim("channel")).alias("channel_title"),
        F.lower("channel").alias("channel_lower"),
    )
)

enriched = (
    valid
    .filter(F.col("starts_on") <= "2024-10-02")
    .select(
        "id",
        "promo_code",
        "discount_pct",
        "channel_lower",
        F.ntile(10).over(w).alias("discount_pct_bucket"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.cume_dist().over(w_recent).alias("discount_pct_cume_dist"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w4)), 4).alias("discount_pct_share"),
    )
)

kept_keys = (
    enriched
    .select("promo_code", "id")
    .intersect(
        df_promotions
        .filter(F.col("promo_code") == "WELCOME5")
        .select("promo_code", "id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .withColumn("discount_pct_bucket_share", F.round((F.col("discount_pct_bucket") / F.sum("discount_pct_bucket").over(w4)), 4))
    .withColumn("discount_pct_bucket_share_2", F.round((F.col("discount_pct_bucket") / F.sum("discount_pct_bucket").over(w4)), 4))
    .withColumn("discount_pct_bucket_rank", F.rank().over(w5))
    .withColumn("row_seq_dense_rank", F.dense_rank().over(w6))
)

metrics_promotions = (
    metrics
    .select(
        "id",
        "promo_code",
        "discount_pct_bucket",
        "row_seq",
        "discount_pct_cume_dist",
        "discount_pct_share",
        "discount_pct_bucket_share",
        "discount_pct_bucket_rank",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w4)), 4).alias("discount_pct_share_2"),
    )
)

base = (
    metrics_promotions
    .filter(F.col("discount_pct_bucket_share") <= 0.42)
    .select(
        "*",
        F.round((F.col("row_seq") * 0.85), 2).alias("row_seq_adjusted"),
        (F.col("discount_pct_cume_dist") + F.col("discount_pct_bucket")).alias("discount_pct_cume_dist_plus_discount_pct_bucket"),
    )
)

filtered = (
    base
    .filter(F.col("row_seq_adjusted") < 79)
    .select(
        "id",
        "promo_code",
        "discount_pct_bucket",
        "row_seq",
        "discount_pct_cume_dist",
        "discount_pct_share",
        "discount_pct_bucket_share",
        "row_seq_adjusted",
        "discount_pct_cume_dist_plus_discount_pct_bucket",
        F.round("discount_pct_bucket_rank", 0).alias("discount_pct_bucket_rank_rounded"),
        F.round((F.col("discount_pct_bucket_share") * 100), 2).alias("discount_pct_bucket_share_adjusted"),
    )
)

totals = (
    filtered
    .groupBy("promo_code")
    .agg(
        F.min("discount_pct_share").alias("min_discount_pct_share"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("promo_code") == "WELCOME5", 1)).alias("welcome5_count"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes_2"),
    )
)

result = (
    totals
    .select(
        "promo_code",
        "min_discount_pct_share",
        "num_promo_codes",
        "distinct_ids",
        "welcome5_count",
        "num_promo_codes_2",
        F.when(F.col("promo_code").rlike("^(WELCOME5|BUNDLE)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("promo_code_zone"),
    )
    .orderBy(F.col("welcome5_count").asc_nulls_first())
)
