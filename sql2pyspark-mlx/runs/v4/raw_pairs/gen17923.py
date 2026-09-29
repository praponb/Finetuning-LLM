from pyspark.sql import functions as F

with_window = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    with_window
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "promo_id",
        "promotion_discount_pct",
        F.when(F.col("promo_code").rlike("^(VIP|WELCOME5)$"), "PRIORITY").when(F.col("promo_code").rlike("^(SPRING10|BUNDLE)$"), "CORE").otherwise("GROUP_B").alias("promo_code_zone"),
    )
    .orderBy(F.col("promo_code").desc_nulls_first(), "promotion_discount_pct")
)
