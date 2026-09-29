from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("rank_in_group", "id", "promo_id")
w3 = Window.orderBy("ends_on", "id", "promo_id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("promotion_discount_pct", "id", "promo_id")
w5 = Window.orderBy("discount_pct", "id", "promo_id").rowsBetween(-5, Window.currentRow)
w_recent = Window.partitionBy("channel").orderBy("promo_id", "id", "promo_id")
w7 = Window.partitionBy("channel").orderBy(F.col("promotion_discount_pct_bucket").desc(), "id", "promo_id")
w8 = Window.partitionBy("promotion_discount_pct_avg_3_tier").orderBy(F.col("avg_discount_pct").desc())

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        F.month("starts_on").alias("starts_on_month"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

matched = (
    ranked
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

deduped = (
    matched
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

scored = (
    deduped
    .filter(F.col("discount_pct") >= 0.27)
    .select(
        "id",
        "channel",
        "discount_pct",
        "starts_on_month",
        "channel_filled",
        "rank_in_group",
        "promo_id",
        "row_num",
        F.avg("promotion_discount_pct").over(w3).alias("promotion_discount_pct_avg_3"),
        F.ntile(4).over(w4).alias("promotion_discount_pct_bucket"),
        F.avg("promo_id").over(w5).alias("promo_id_avg_5"),
        F.ntile(5).over(w_recent).alias("promo_id_bucket"),
    )
)

ranked_promotions = (
    scored
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

cleaned = (
    ranked_promotions
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "discount_pct",
        "channel_filled",
        "rank_in_group",
        "promo_id",
        "row_num",
        "promotion_discount_pct_avg_3",
        "promotion_discount_pct_bucket",
        "promo_id_avg_5",
        "promo_id_bucket",
        F.floor("promo_id_bucket").alias("promo_id_bucket_whole"),
        F.coalesce("promotion_discount_pct_bucket", F.lit(0)).alias("promotion_discount_pct_bucket_filled"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("promotion_discount_pct_avg_3") >= 0, "LARGE").otherwise("SMALL").alias("promotion_discount_pct_avg_3_tier"),
        "promo_id",
    )
    .agg(
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.percentile_approx("discount_pct", 0.9).alias("discount_pct_p90"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("avg_discount_pct") < 1000000)
    .withColumn("avg_discount_pct_rank", F.rank().over(w8))
)
