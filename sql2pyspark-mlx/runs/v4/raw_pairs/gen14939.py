from pyspark.sql import functions as F

attributed = (
    df_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    attributed
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "promo_id",
        "promotion_discount_pct",
        F.when(F.col("promo_code").rlike("^(BUNDLE|SPRING10)$"), "CORE").when(F.col("promo_code").rlike("^(VIP|WELCOME5)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("promo_code_zone"),
    )
)
