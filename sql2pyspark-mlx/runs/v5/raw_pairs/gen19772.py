from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("promotion_discount_pct_2", "id", "promo_id", "promo_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promotion_promo_code_2", "promo_code").orderBy("starts_on", "id", "promo_id", "promo_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code")
w4 = Window.partitionBy("promo_code").orderBy(F.col("cumulative_promo_id_2").desc(), "id", "promo_id", "promo_id_2")

matched = (
    df_promotions
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window = (
    matched
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

scored = (
    with_window
    .filter(F.col("promo_id_2") != 9)
    .select(
        "*",
        F.sum("promo_id_2").over(w_running).alias("cumulative_promo_id_2"),
        F.sum("promotion_discount_pct").over(w2).alias("running_promotion_discount_pct"),
        F.min("discount_pct").over(w3).alias("promo_code_min_discount_pct"),
    )
)

ranked = (
    scored
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy("channel", F.date_trunc("MONTH", "ends_on").alias("activity_month"))
    .agg(
        F.round(F.avg("promo_id"), 2).alias("avg_promo_id"),
        F.round(F.sum("running_promotion_discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("running_promotion_discount_pct_per_mobile"),
        F.sum(F.when(F.col("channel") == "partner", F.col("running_promotion_discount_pct")).otherwise(0)).alias("partner_running_promotion_discount_pct"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
    .filter(F.col("running_promotion_discount_pct_per_mobile") >= 0)
)
