from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("channel", "promotion_promo_code").orderBy("starts_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promotion_promo_code").orderBy("discount_pct", "id", "promo_id")
w3 = Window.partitionBy("promotion_promo_code").orderBy("cumulative_promotion_discount_pct", "id", "promo_id")
w4 = Window.partitionBy("channel").orderBy("cumulative_promotion_discount_pct", "id", "promo_id")
w5 = Window.partitionBy("promotion_promo_code").orderBy("row_seq", "id", "promo_id").rowsBetween(-5, -1)
w6 = Window.partitionBy("channel", "promotion_promo_code").orderBy(F.col("promotion_discount_pct").desc_nulls_first())
w7 = Window.partitionBy("channel").orderBy("discount_pct", "id", "promo_id").rowsBetween(-5, Window.currentRow)

cleaned = (
    df_promotions
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-12-16"))) & F.col("channel").isin(["web", "partner"]))
    .select("*", F.lower("channel").alias("channel_lower"))
)

attributed = (
    cleaned
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

scored = (
    attributed
    .withColumn("cumulative_promotion_discount_pct", F.sum("promotion_discount_pct").over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w2))
)

scored_promotions = (
    scored
    .withColumn("prev_promo_id", F.lag("promo_id", 2).over(w3))
    .withColumn("next_cumulative_promotion_discount_pct", F.lead("cumulative_promotion_discount_pct", 1).over(w4))
    .withColumn("discount_pct_avg_5_prior", F.avg("discount_pct").over(w5))
    .withColumn("promotion_discount_pct_percent_rank", F.percent_rank().over(w6))
    .withColumn("promo_id_avg_5", F.avg("promo_id").over(w7))
)

result = (
    scored_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "channel_lower",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
    )
)
