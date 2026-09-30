from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter(F.col("ends_on").between("2024-01-01", "2025-07-01"))
    .withColumn("channel_title", F.initcap(F.trim("channel")))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("OTHER")))
)

result = (
    cleaned
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)
