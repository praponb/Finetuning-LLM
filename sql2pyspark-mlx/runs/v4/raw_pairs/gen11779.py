from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")
w2 = Window.partitionBy("promotion_promo_code")
w3 = Window.partitionBy("promo_code")

deduped = (
    df_promotions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

attributed = (
    deduped
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

kept_keys = (
    attributed
    .select("id")
    .subtract(
        df_promotions
        .filter(F.month("starts_on").isin([7, 8, 11]))
        .select("id")
    )
)

kept_rows = (
    attributed
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("promo_code") == "VIP")
    .select(
        "promo_code",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_promo_code",
        F.month("ends_on").alias("ends_on_month"),
    )
)

scored = (
    base
    .withColumn("promo_id_share", F.round((F.col("promo_id") / F.sum("promo_id").over(w2)), 4))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4))
    .withColumn("promo_code_count_discount_pct", F.count("discount_pct").over(w3))
)

result = (
    scored
    .select(
        "promo_code",
        "ends_on",
        "promo_id",
        "promo_id_share",
        "discount_pct_share",
        F.when(F.col("promo_code").rlike("^(WELCOME5|SPRING10)$"), "CORE").when(F.col("promo_code").rlike("^(VIP|BUNDLE)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("promo_code_zone"),
    )
    .orderBy(F.col("ends_on").desc_nulls_first())
)
