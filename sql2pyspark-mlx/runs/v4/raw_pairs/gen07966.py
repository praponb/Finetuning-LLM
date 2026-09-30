from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")
w3 = Window.partitionBy("channel").orderBy(F.col("rn").desc(), "id")
w4 = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w5 = Window.partitionBy("promo_code").orderBy("rn_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_promotions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked_promotions = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

base = (
    ranked_promotions
    .filter(F.col("channel").isNotNull())
    .withColumn("rn_whole", F.ceil("rn"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("N/A")))
)

scored = (
    base
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("next_rn_whole", F.lead("rn_whole", 1).over(w4))
    .withColumn("running_discount_pct", F.sum("discount_pct").over(w5))
)

matched_keys = (
    scored
    .select("channel", "promo_code")
    .exceptAll(
        df_promotions
        .filter(F.col("starts_on") >= "2024-03-10")
        .select("channel", "promo_code")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

attributed = (
    reconciled
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & F.col("e.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

summary = (
    attributed
    .cube(
        F.when(F.col("rn_whole") >= 67, "PREMIUM").otherwise("STANDARD").alias("rn_whole_tier"),
        F.when(F.col("next_rn_whole") >= 79, "PREMIUM").otherwise("STANDARD").alias("next_rn_whole_tier"),
        F.when(F.col("row_seq") >= 9, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.count(F.when(F.col("promotion_promo_code") == "SPRING10", 1)).alias("spring10_rows"),
        F.sum("promotion_discount_pct").alias("total_promotion_discount_pct"),
        F.sum(F.when(F.col("promo_code") == "BUNDLE", F.col("promotion_discount_pct"))).alias("bundle_total_promotion_discount_pct"),
    )
)

result = (
    summary
    .select(
        "rn_whole_tier",
        "next_rn_whole_tier",
        "row_seq_tier",
        "mobile_rows",
        "spring10_rows",
        "total_promotion_discount_pct",
        "bundle_total_promotion_discount_pct",
    )
    .orderBy(F.col("spring10_rows").asc_nulls_first())
)
