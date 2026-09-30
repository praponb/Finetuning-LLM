from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "promotion_promo_code").orderBy(F.col("promotion_discount_pct").desc_nulls_last(), "id", "promo_id")
w2 = Window.partitionBy("channel").orderBy(F.col("ends_on").desc())
w3 = Window.partitionBy("channel")
w4 = Window.orderBy(F.col("promo_id").desc(), "id", "promo_id")
w5 = Window.partitionBy("promotion_promo_code").orderBy("latest_rank", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("promotion_promo_code", "channel").orderBy("ends_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("promo_id_share", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("channel").orderBy("starts_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("promotion_promo_code").orderBy(F.col("first_known_running_latest_rank_adjusted").desc_nulls_last())
w10 = Window.partitionBy("promotion_promo_code").orderBy("latest_rank_adjusted", "id", "promo_id")
w11 = Window.partitionBy("promotion_promo_code").orderBy("starts_on", "id", "promo_id")

matched = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

deduped = (
    matched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "promo_id",
        "promotion_promo_code",
    )
)

latest = (
    deduped
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

valid = (
    latest
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.round((F.col("latest_rank") * 100), 2).alias("latest_rank_adjusted"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "channel",
        "starts_on",
        "promo_id",
        "promotion_promo_code",
        "latest_rank_adjusted",
        F.round((F.col("promo_id") / F.sum("promo_id").over(w3)), 4).alias("promo_id_share"),
        F.ntile(10).over(w4).alias("promo_id_bucket"),
        F.last("promo_id", ignorenulls=True).over(w5).alias("last_known_promo_id"),
        F.sum("latest_rank_adjusted").over(w_running).alias("running_latest_rank_adjusted"),
    )
)

enriched_promotions = (
    enriched
    .withColumn("running_last_known_promo_id", F.sum("last_known_promo_id").over(w7))
    .withColumn("first_known_running_latest_rank_adjusted", F.first("running_latest_rank_adjusted", ignorenulls=True).over(w8))
)

metrics = (
    enriched_promotions
    .select(
        "id",
        "channel",
        "promo_id",
        "promotion_promo_code",
        "latest_rank_adjusted",
        "promo_id_bucket",
        "last_known_promo_id",
        "running_last_known_promo_id",
        "first_known_running_latest_rank_adjusted",
        F.cume_dist().over(w9).alias("first_known_running_latest_rank_adjusted_cume_dist"),
        F.lag("promo_id_bucket", 2).over(w10).alias("prev_promo_id_bucket"),
        F.lead("running_last_known_promo_id", 1).over(w11).alias("next_running_last_known_promo_id"),
    )
)

result = (
    metrics
    .withColumn("promo_id_bucket_tier", F.when(F.col("promo_id_bucket") >= 7, "LARGE").otherwise("SMALL"))
    .cube("promo_id_bucket_tier", "next_running_last_known_promo_id")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("running_last_known_promo_id") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("running_last_known_promo_id_per_web"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy(F.col("promo_id_bucket_tier").desc())
)
