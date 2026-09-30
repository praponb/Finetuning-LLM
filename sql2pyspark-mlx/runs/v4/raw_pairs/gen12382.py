from pyspark.sql import functions as F

matched = (
    df_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") <= F.date_add(F.col("pr.ends_on"), 3)), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    matched
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
    )
    .orderBy(F.col("promo_id").asc_nulls_last(), "ends_on")
)
