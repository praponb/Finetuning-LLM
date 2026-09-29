from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter(F.col("promo_code").isin(["SPRING10", "WELCOME5"]))
    .withColumn("discount_pct_band", F.when(F.col("discount_pct") >= 0.35, "High").when(F.col("discount_pct") >= 0.13, "Medium").otherwise("Low"))
    .withColumn("promo_code_channel", F.concat("promo_code", F.lit(", "), "channel"))
)

attributed = (
    filtered
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window = (
    attributed
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("e.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

valid = (
    with_window
    .filter(F.col("promotion_promo_code_2").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct_band",
        "promo_code_channel",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "promo_id_2",
        "promotion_discount_pct_2",
        F.col("promo_id_2").cast("string").alias("promo_id_2_str"),
        F.coalesce("promotion_promo_code_2", F.lit("DIRECT")).alias("resolved_promotion_promo_code_2"),
    )
)

matched = (
    valid
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .groupBy("channel")
    .pivot("promo_code", ["VIP", "BUNDLE"])
    .agg(
        F.sum("promotion_discount_pct_2").alias("amount"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "channel",
        F.col("VIP_amount").alias("vip_amount"),
        F.col("VIP_item_count").alias("vip_item_count"),
        F.col("BUNDLE_amount").alias("bundle_amount"),
        F.col("BUNDLE_item_count").alias("bundle_item_count"),
    )
)
