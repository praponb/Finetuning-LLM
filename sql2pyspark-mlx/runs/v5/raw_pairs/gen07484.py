from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w3 = Window.partitionBy("channel", "promo_code").orderBy(F.col("next_discount_pct").asc_nulls_first(), "id")
w4 = Window.partitionBy("promo_code").orderBy(F.col("promo_id").desc_nulls_first(), "id", "promo_id")

base = (
    df_promotions
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-08-12"))) & F.col("promo_code").isin(["SPRING10", "BUNDLE"]))
    .withColumn("discount_pct_level", F.when(F.col("discount_pct") >= 0.35, "High").when(F.col("discount_pct") >= 0.16, "Medium").otherwise("Low"))
)

scored = (
    base
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        F.lead("discount_pct").over(w).alias("next_discount_pct"),
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

matched = (
    ranked
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

ranked_promotions = (
    matched
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

wide = (
    ranked_promotions
    .groupBy("promotion_promo_code", "promo_code")
    .pivot("channel", ["web", "mobile"])
    .agg(F.sum("next_discount_pct").alias("amount"))
)

result = wide.select("promotion_promo_code", "promo_code", "web")
