from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-05-14").cast("date")) & F.col("channel").isNotNull() & F.col("discount_pct").between(0.2, 0.22))
    .select("*", F.date_format("starts_on", "yyyy-MM").alias("starts_on_label"))
)

result = (
    filtered
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)
