from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "promotion_promo_code").orderBy(F.col("promo_id").desc(), "id", "promo_id")

with_window = (
    df_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

ranked = (
    with_window
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

cleaned = (
    ranked
    .filter(F.col("ends_on") > F.lit("2023-10-08").cast("date"))
    .select(
        "*",
        F.round("promotion_discount_pct", 1).alias("promotion_discount_pct_rounded"),
        F.coalesce("promotion_promo_code", F.lit("NONE")).alias("resolved_promotion_promo_code"),
    )
)

with_window_promotions = (
    cleaned
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

cleaned_promotions = (
    with_window_promotions
    .filter(F.col("promo_id_2") > 7)
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct",
        "promotion_discount_pct_rounded",
        "resolved_promotion_promo_code",
        "promo_id_2",
        "promotion_promo_code_2",
        "promotion_discount_pct_2",
        F.when(F.col("promotion_discount_pct_2") >= 0.3, "High").when(F.col("promotion_discount_pct_2") >= 0.12, "Medium").otherwise("Low").alias("promotion_discount_pct_2_tier"),
        F.coalesce("promotion_promo_code_2", F.lit("NONE")).alias("promotion_promo_code_2_filled"),
    )
)

result = (
    cleaned_promotions
    .groupBy("promo_id")
    .agg(
        F.sum(F.when(F.col("promotion_promo_code_2") == "BUNDLE", F.col("promotion_discount_pct_rounded")).otherwise(0)).alias("bundle_promotion_discount_pct_rounded"),
        F.round(F.sum("promotion_discount_pct_2") / F.nullif(F.count(F.when(F.col("promotion_promo_code_2") == "BUNDLE", 1)), F.lit(0)), 2).alias("promotion_discount_pct_2_per_bundle"),
        F.round(F.sum("promotion_discount_pct") / F.nullif(F.count(F.when(F.col("promotion_promo_code_2") == "WELCOME5", 1)), F.lit(0)), 2).alias("promotion_discount_pct_per_welcome5"),
        F.sum(F.when(F.col("promotion_promo_code_2") == "VIP", F.col("promotion_discount_pct_2"))).alias("vip_total_promotion_discount_pct_2"),
    )
    .orderBy("bundle_promotion_discount_pct_rounded")
)
