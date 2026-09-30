from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("promo_id_2").desc(), "id", "promo_id", "promo_id_2")
w2 = Window.partitionBy("promotion_promo_code")
w3 = Window.partitionBy("promotion_promo_code_2", "channel")

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

cleaned = (
    with_window
    .filter(F.col("channel").isin(["mobile", "store"]) & (F.col("starts_on") > F.to_date(F.lit("2023-04-04"))))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        (F.col("promotion_discount_pct") - F.col("discount_pct")).alias("promotion_discount_pct_minus_discount_pct"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

with_window_promotions = (
    cleaned
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct"),
    )
)

ranked = (
    with_window_promotions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

remaining_keys = (
    ranked
    .select("id")
    .intersect(
        df_promotions
        .filter(F.col("discount_pct") <= 0.2)
        .select("id")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "*",
        F.round((F.col("promotion_discount_pct") / F.sum("promotion_discount_pct").over(w2)), 4).alias("promotion_discount_pct_share"),
        F.round((F.col("promo_id_2") / F.sum("promo_id_2").over(w3)), 4).alias("promo_id_2_share"),
    )
)

result = (
    with_windows
    .rollup(F.date_trunc("quarter", "ends_on").alias("quarter_start"))
    .agg(
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promotion_promo_code_2") == "VIP", 1)), F.lit(0)), 2).alias("discount_pct_per_vip"),
        F.max("promotion_discount_pct_share").alias("max_promotion_discount_pct_share"),
    )
    .orderBy(F.col("quarter_start").asc_nulls_first())
)
