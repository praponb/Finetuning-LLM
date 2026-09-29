from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter(~F.col("channel").eqNullSafe(F.lit("mobile")))
    .withColumn("discount_pct_whole", F.floor("discount_pct"))
)

matched = (
    filtered
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & F.col("e.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window = (
    matched
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

matched_keys = (
    with_window
    .select("id", "promo_code")
    .subtract(
        df_promotions
        .filter(F.col("ends_on") < "2024-12-01")
        .select("id", "promo_code")
    )
)

verified = (
    with_window
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct_whole",
        "promotion_promo_code",
        "promo_id_2",
        "promotion_promo_code_2",
        "promotion_discount_pct_2",
        F.when(F.col("promo_code").rlike("^(WELCOME5|BUNDLE)$"), "PRIORITY").when(F.col("promo_code").rlike("^(SPRING10|VIP)$"), "GROUP_B").otherwise("CORE").alias("promo_code_zone"),
    )
    .orderBy("promo_id_2")
)
