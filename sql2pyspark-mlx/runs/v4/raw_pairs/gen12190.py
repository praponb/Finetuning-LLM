from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w_hist = Window.partitionBy("channel").orderBy("discount_pct", "id")
w3 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("starts_on", "id")

enriched = (
    df_promotions
    .select(
        "*",
        F.lead("discount_pct", 1).over(w).alias("next_discount_pct"),
        F.ntile(10).over(w_hist).alias("discount_pct_bucket"),
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_6"),
        F.lead("discount_pct", 2).over(w4).alias("next_discount_pct_2"),
    )
)

matched = (
    enriched
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched_promotions = (
    matched
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & F.col("e.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

result = (
    matched_promotions
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        "next_discount_pct",
        "discount_pct_bucket",
        "discount_pct_avg_6",
        "promo_id",
        "promo_id_2",
    )
    .orderBy(F.col("promo_id").asc_nulls_last(), F.col("starts_on").desc())
)
