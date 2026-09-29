from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w4 = Window.partitionBy("promo_code").orderBy(F.col("promo_code_min_discount_pct").asc_nulls_first())
w5 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w6 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_share").desc(), "id")
w7 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc())
w8 = Window.partitionBy("channel").orderBy("discount_pct_share", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_promotions
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-10-03"))) & F.col("channel").isNotNull())
    .withColumn("promo_code_title", F.initcap(F.trim("promo_code")))
)

scored = (
    base
    .withColumn("promo_code_min_discount_pct", F.min("discount_pct").over(w))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4))
    .withColumn("next_discount_pct", F.lead("discount_pct", 1).over(w3))
)

scored_promotions = (
    scored
    .select(
        "*",
        F.cume_dist().over(w4).alias("promo_code_min_discount_pct_cume_dist"),
        F.lead("discount_pct_share", 2).over(w5).alias("next_discount_pct_share"),
        F.ntile(4).over(w6).alias("discount_pct_share_bucket"),
        F.cume_dist().over(w7).alias("discount_pct_cume_dist"),
        F.avg("promo_code_min_discount_pct").over(w8).alias("promo_code_min_discount_pct_avg_6"),
    )
)

result = (
    scored_promotions
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "promo_code_title",
        "promo_code_min_discount_pct",
        "next_discount_pct",
        "promo_code_min_discount_pct_cume_dist",
        F.when(F.col("channel").rlike("^(store|web)$"), "GROUP_B").when(F.col("channel").rlike("^(mobile|partner)$"), "GROUP_A").otherwise("EXTENDED").alias("channel_zone"),
    )
    .orderBy(F.col("promo_code_min_discount_pct_cume_dist").asc_nulls_last(), "id")
)
