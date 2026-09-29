from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code").orderBy(F.col("promo_id").desc_nulls_last(), "id", "promo_id")
w2 = Window.partitionBy("channel")
w3 = Window.orderBy("promotion_discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("channel_min_promotion_discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("channel_min_promotion_discount_pct")
w6 = Window.orderBy(F.col("promo_id_whole").desc(), "id", "promo_id")

with_window = (
    df_promotions
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

ranked = (
    with_window
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "channel", "promo_id", "promotion_discount_pct")
)

with_windows = (
    ranked
    .select(
        "id",
        "promo_id",
        F.min("promotion_discount_pct").over(w2).alias("channel_min_promotion_discount_pct"),
        F.sum("promotion_discount_pct").over(w3).alias("running_promotion_discount_pct"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("promo_id") < 5)
    .select(
        "*",
        F.greatest("channel_min_promotion_discount_pct", "running_promotion_discount_pct").alias("greatest_channel_min_promotion_discount_pct_running_promotion_discount_pct"),
        F.floor("promo_id").alias("promo_id_whole"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.first("running_promotion_discount_pct", ignorenulls=True).over(w4).alias("first_known_running_promotion_discount_pct"),
        F.rank().over(w5).alias("channel_min_promotion_discount_pct_rank"),
        F.ntile(10).over(w6).alias("promo_id_whole_bucket"),
    )
)

result = (
    enriched
    .select(
        "id",
        "promo_id",
        "channel_min_promotion_discount_pct",
        "greatest_channel_min_promotion_discount_pct_running_promotion_discount_pct",
        "channel_min_promotion_discount_pct_rank",
        "promo_id_whole_bucket",
    )
    .orderBy(F.col("channel_min_promotion_discount_pct").desc_nulls_first())
)
