from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter(F.col("starts_on") <= "2024-09-18")
    .withColumn("promo_code_prefix", F.substring("promo_code", 1, 4))
    .withColumn("starts_on_month_end", F.last_day("starts_on"))
)

result = (
    filtered
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)
