from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on")
w2 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct_rounded").desc_nulls_first(), "id", "promo_id")

cleaned = (
    df_promotions
    .filter(F.month("starts_on").isin([4, 9, 12]))
    .withColumn("discount_pct_rounded", F.round("discount_pct", 0))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("OTHER")))
)

with_window = (
    cleaned
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

latest = (
    with_window
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "discount_pct_rounded",
        "resolved_channel",
        "promo_id",
        "promotion_promo_code",
    )
)

latest_promotions = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

matched = (
    latest_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("s.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    matched
    .cube(
        F.when(F.col("discount_pct") >= 0.31, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.countDistinct("promotion_promo_code").alias("distinct_promotion_promo_codes"),
        F.avg(F.when(F.col("promotion_promo_code") == "VIP", F.col("discount_pct"))).alias("vip_avg_discount_pct"),
        F.round(F.sum("discount_pct_rounded") / F.nullif(F.count(F.when(F.col("promotion_promo_code") == "VIP", 1)), F.lit(0)), 2).alias("discount_pct_rounded_per_vip"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
    .orderBy(F.col("distinct_promotion_promo_codes").desc_nulls_last())
)
