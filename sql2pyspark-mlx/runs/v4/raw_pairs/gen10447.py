from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id", "promo_id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("promotion_promo_code").orderBy(F.col("promo_id").asc_nulls_last())
w3 = Window.orderBy(F.col("discount_pct").desc(), "id", "promo_id")
w4 = Window.partitionBy("channel")
w5 = Window.partitionBy("channel").orderBy(F.col("discount_pct_avg_2").asc_nulls_last())
w6 = Window.partitionBy("promotion_promo_code").orderBy("promo_id_share", "id", "promo_id")
w7 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_avg_2").desc_nulls_last(), "id", "promo_id")

with_window = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("e.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

enriched = (
    with_window
    .select(
        "*",
        F.avg("discount_pct").over(w).alias("discount_pct_avg_2"),
        F.percent_rank().over(w2).alias("promo_id_percent_rank"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

enriched_promotions = (
    enriched
    .withColumn("promo_id_share", F.round((F.col("promo_id") / F.sum("promo_id").over(w4)), 4))
    .withColumn("discount_pct_avg_2_percent_rank", F.percent_rank().over(w5))
)

latest = (
    enriched_promotions
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

latest_promotions = (
    latest
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

result = (
    latest_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct",
        "discount_pct_avg_2",
        "promo_id_percent_rank",
        F.when(F.col("promo_code").rlike("^(BUNDLE|SPRING10)$"), "GROUP_A").otherwise("PRIORITY").alias("promo_code_zone"),
    )
    .orderBy("channel", "discount_pct_avg_2")
)
