from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w2 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(-5, -1)
w_ordered = Window.partitionBy("promo_code").orderBy("discount_pct")
w5 = Window.partitionBy("promo_code", "promotion_promo_code_4").orderBy("discount_pct", "id", "promo_id", "promo_id_2", "promo_id_3", "promo_id_4").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("promotion_promo_code_4").orderBy("ends_on", "id", "promo_id", "promo_id_2", "promo_id_3", "promo_id_4")
w7 = Window.partitionBy("promotion_promo_code_4").orderBy(F.col("promotion_discount_pct_2").desc())

enriched = (
    df_promotions
    .withColumn("discount_pct_dense_rank", F.dense_rank().over(w))
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w2))
    .withColumn("discount_pct_avg_5_prior", F.avg("discount_pct").over(w_part))
    .withColumn("discount_pct_percent_rank", F.percent_rank().over(w_ordered))
)

matched = (
    enriched
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched_promotions = (
    matched
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

matched_promotions_2 = (
    matched_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & F.col("e.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

with_window = (
    matched_promotions_2
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_4"),
        F.col("pr.promo_code").alias("promotion_promo_code_4"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_4"),
    )
)

scored = (
    with_window
    .select(
        "*",
        F.sum("promotion_discount_pct_2").over(w5).alias("cumulative_promotion_discount_pct_2"),
        F.row_number().over(w6).alias("event_seq"),
        F.rank().over(w7).alias("promotion_discount_pct_2_rank"),
    )
)

result = (
    scored
    .groupBy("promotion_promo_code_3")
    .agg(
        F.countDistinct("promotion_promo_code_4").alias("distinct_promotion_promo_code_4s"),
        F.size(F.collect_set("promotion_promo_code")).alias("num_promotion_promo_codes"),
    )
    .filter(F.col("num_promotion_promo_codes") >= 1)
    .orderBy(F.col("promotion_promo_code_3").desc())
)
