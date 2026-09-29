from pyspark.sql import functions as F

base = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-12-20").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.ceil("discount_pct").alias("discount_pct_whole"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

attributed = (
    base
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window = (
    attributed
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") <= F.date_add(F.col("pr.ends_on"), 7)), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

result = (
    with_window
    .groupBy("promotion_promo_code_2", "channel")
    .pivot("promo_code", ["WELCOME5", "BUNDLE"])
    .agg(F.max("promo_id").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
