from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter(~F.col("channel").eqNullSafe(F.lit("store")))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.substring("promo_code", 1, 3).alias("promo_code_prefix"),
        F.upper("channel").alias("channel_upper"),
    )
)

valid_promotions = (
    valid
    .filter((F.col("starts_on") >= F.lit("2023-05-20").cast("date")) & (F.col("channel") != "store"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "promo_code_prefix",
        "channel_upper",
        F.when(F.col("promo_code") == "BUNDLE", 1).otherwise(0).alias("is_bundle"),
    )
)

valid_promotions_2 = (
    valid_promotions
    .filter(F.col("promo_code").isin(["WELCOME5", "BUNDLE", "VIP"]))
    .withColumn("promo_code_group", F.when(F.col("promo_code").isin(["SPRING10", "BUNDLE"]), "Tier 1").otherwise("Other"))
    .withColumn("is_bundle_whole", F.floor("is_bundle"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("OTHER")))
)

with_window = (
    valid_promotions_2
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    with_window
    .groupBy("channel")
    .pivot("promo_code", ["VIP", "BUNDLE"])
    .agg(F.avg("is_bundle_whole"))
)
