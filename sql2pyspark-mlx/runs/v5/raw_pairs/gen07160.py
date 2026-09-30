from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code", "channel").orderBy(F.col("promo_id").desc())
w_grp = Window.partitionBy("promo_code").orderBy("ends_on", "id", "promo_id")
w_grp_2 = Window.partitionBy("promo_code", "promotion_promo_code").orderBy("ends_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promotion_promo_code").orderBy("next_discount_pct", "id", "promo_id")
w5 = Window.partitionBy("channel", "promotion_promo_code")
w6 = Window.partitionBy("channel", "promo_code").orderBy(F.col("prev_discount_pct").desc_nulls_last(), "id", "promo_id")

attributed = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

deduped = (
    attributed
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    deduped
    .filter(F.col("promo_id") != 6)
    .withColumn("next_promotion_discount_pct", F.lead("promotion_discount_pct").over(w_grp))
    .withColumn("next_discount_pct", F.lead("discount_pct").over(w_grp))
    .withColumn("prev_discount_pct", F.lag("discount_pct").over(w_grp))
    .withColumn("seq_no", F.row_number().over(w_grp))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w_grp_2))
)

with_windows = (
    scored
    .withColumn("prev_seq_no", F.lag("seq_no").over(w4))
    .withColumn("prev_prev_discount_pct", F.lag("prev_discount_pct").over(w4))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("prev_discount_pct_share", F.round((F.col("prev_discount_pct") / F.sum("prev_discount_pct").over(w5)), 4))
)

ranked = (
    with_windows
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select(
        "id",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct",
        "dedup_rank",
        "next_promotion_discount_pct",
        "next_discount_pct",
        "prev_discount_pct",
        "last_known_channel",
    )
    .orderBy("next_promotion_discount_pct")
)
