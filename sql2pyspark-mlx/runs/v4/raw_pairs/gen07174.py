from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.orderBy(F.col("discount_pct").desc())
w3 = Window.orderBy("discount_pct")

kept_keys = (
    df_promotions
    .select("promo_code")
    .subtract(
        df_promotions
        .filter(F.col("promo_code").isin(["WELCOME5", "VIP"]))
        .select("promo_code")
    )
)

verified = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4).alias("discount_pct_share"),
        F.dense_rank().over(w2).alias("discount_pct_dense_rank"),
        F.dense_rank().over(w3).alias("discount_pct_dense_rank_2"),
    )
)

with_window = (
    enriched
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") <= F.date_add(F.col("pr.ends_on"), 7)), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

wide = (
    with_window
    .groupBy("channel")
    .pivot("promotion_promo_code", ["VIP", "BUNDLE", "WELCOME5"])
    .agg(F.avg("discount_pct_dense_rank_2").alias("value_sum"))
    .select(
        "channel",
        F.col("VIP").alias("vip"),
        F.col("BUNDLE").alias("bundle"),
        F.col("WELCOME5").alias("welcome5"),
    )
)

result = (
    wide
    .select("channel", "vip", "welcome5")
    .orderBy(F.col("vip").desc())
)
