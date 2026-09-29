from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("channel")
w2 = Window.partitionBy("channel", "promotion_promo_code_2").orderBy(F.col("promo_id_2").desc_nulls_first())
w3 = Window.partitionBy("promotion_promo_code_2").orderBy(F.col("promotion_discount_pct_2").desc_nulls_last())
w4 = Window.orderBy("promotion_discount_pct_2")
w5 = Window.partitionBy("promotion_promo_code")
w6 = Window.orderBy(F.col("promo_id").desc_nulls_last())

attributed = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

kept_keys = (
    attributed
    .select("promo_code", "channel")
    .subtract(
        df_promotions
        .filter(F.month("ends_on").isin([2, 9, 11]))
        .select("promo_code", "channel")
    )
)

kept_rows = (
    attributed
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

matched = (
    kept_rows
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

with_windows = (
    matched
    .withColumn("channel_stddev_samp_promo_id", F.stddev_samp("promo_id").over(w_hist))
    .withColumn("promo_id_2_percent_rank", F.percent_rank().over(w2))
    .withColumn("promotion_discount_pct_2_rank", F.rank().over(w3))
)

with_windows_promotions = (
    with_windows
    .withColumn("promotion_discount_pct_2_dense_rank", F.dense_rank().over(w4))
    .withColumn("promotion_discount_pct_2_rank_share", F.round((F.col("promotion_discount_pct_2_rank") / F.sum("promotion_discount_pct_2_rank").over(w5)), 4))
    .withColumn("promo_id_rank", F.rank().over(w6))
)

kept_keys_2 = (
    with_windows_promotions
    .select("channel", "id")
    .subtract(
        df_promotions
        .filter(F.col("promo_code").isin(["WELCOME5", "BUNDLE"]))
        .select("channel", "id")
    )
)

verified = (
    with_windows_promotions
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

totals = (
    verified
    .groupBy(
        F.when(F.col("promotion_discount_pct_2_rank_share") >= 0.63, "PREMIUM").otherwise("STANDARD").alias("promotion_discount_pct_2_rank_share_tier"),
        F.date_trunc("YEAR", "starts_on").alias("year_start"),
    )
    .agg(
        F.round(F.avg("promo_id_2_percent_rank"), 2).alias("avg_promo_id_2_percent_rank"),
        F.size(F.collect_set("promotion_promo_code_2")).alias("num_promotion_promo_code_2s"),
    )
)

result = (
    totals
    .select(
        "promotion_discount_pct_2_rank_share_tier",
        "year_start",
        "avg_promo_id_2_percent_rank",
        "num_promotion_promo_code_2s",
    )
    .orderBy(F.col("year_start").desc_nulls_last())
)
