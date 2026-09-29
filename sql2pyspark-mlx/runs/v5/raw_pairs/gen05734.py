from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code_2", "promo_code").orderBy(F.col("promo_code_length").desc(), "id", "promo_id", "promo_id_2", "promo_id_3")

matched = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

valid = (
    matched
    .filter(F.col("channel") == "partner")
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        F.length("promo_code").alias("promo_code_length"),
        F.when(F.col("discount_pct") >= 0.33, "High").when(F.col("discount_pct") >= 0.12, "Medium").otherwise("Low").alias("discount_pct_band"),
    )
)

matched_promotions = (
    valid
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

matched_promotions_2 = (
    matched_promotions
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("src.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

base = (
    matched_promotions_2
    .filter(F.col("starts_on") >= F.lit("2023-03-22").cast("date"))
    .select(
        "*",
        F.round((F.col("promo_id") / F.col("promotion_discount_pct_3")), 2).alias("promo_id_per_promotion_discount_pct_3"),
        F.coalesce("promotion_promo_code_2", F.lit("NONE")).alias("promotion_promo_code_2_clean"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "channel",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct",
        "promo_code_length",
        "promo_id_2",
        "promotion_discount_pct_2",
        "promo_id_3",
        "promotion_discount_pct_3",
        "promo_id_per_promotion_discount_pct_3",
    )
)

result = (
    latest
    .withColumn("promo_id_3_tier", F.when(F.col("promo_id_3") >= 9, "LARGE").otherwise("SMALL"))
    .withColumn("promo_id_per_promotion_discount_pct_3_tier", F.when(F.col("promo_id_per_promotion_discount_pct_3") >= 62, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["promo_id", "promo_id_3_tier", "promo_id_per_promotion_discount_pct_3_tier"], ["promo_id", "promo_id_3_tier"]],
        "promo_id",
        "promo_id_3_tier",
        "promo_id_per_promotion_discount_pct_3_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("promo_code_length") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("promo_code_length_per_web"),
        F.avg("promo_code_length").alias("avg_promo_code_length"),
    )
    .filter(F.col("promo_code_length_per_web") > 0)
)
