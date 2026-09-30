from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("week_start").orderBy(F.col("bundle_total_discount_pct_whole").desc())

base = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("promo_code") == "SPRING10"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.upper("promo_code").alias("promo_code_upper"),
    )
)

base_promotions = (
    base
    .filter(F.col("discount_pct") < 0.22)
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        "promo_code_upper",
        F.ceil("discount_pct").alias("discount_pct_whole"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

matched_keys = (
    base_promotions
    .select("id", "channel")
    .intersectAll(
        df_promotions
        .filter(F.year("starts_on") == 2023)
        .select("id", "channel")
    )
)

kept_rows = (
    base_promotions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

with_window = (
    kept_rows
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window_promotions = (
    with_window
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

attributed = (
    with_window_promotions
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & F.col("a.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

result = (
    attributed
    .groupBy(F.date_trunc("WEEK", "starts_on").alias("week_start"), "promo_code")
    .agg(
        F.count_if(F.col("promotion_promo_code") == "VIP").alias("vip_events"),
        F.sum(F.when(F.col("promotion_promo_code") == "BUNDLE", F.col("discount_pct_whole"))).alias("bundle_total_discount_pct_whole"),
    )
    .withColumn("bundle_total_discount_pct_whole_rank", F.rank().over(w))
)
