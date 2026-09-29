from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("week_start").orderBy(F.col("avg_promotion_discount_pct").desc())

base = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-07-29").cast("date")) & (F.col("ends_on") < "2023-12-15"))
    .withColumn("starts_on_minus_9d", F.date_sub("starts_on", 9))
)

attributed = (
    base
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched_keys = (
    attributed
    .select("id")
    .intersect(
        df_promotions
        .filter(F.col("ends_on") < "2025-02-01")
        .select("id")
    )
)

kept_rows = (
    attributed
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys = (
    kept_rows
    .select("id", "promo_code")
    .subtract(
        df_promotions
        .filter(F.col("ends_on") <= "2023-08-06")
        .select("id", "promo_code")
    )
)

verified = (
    kept_rows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

matched = (
    verified
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.starts_on_minus_9d") >= F.col("pr.starts_on")) & (F.col("a.starts_on_minus_9d") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

with_window = (
    matched
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

base_promotions = (
    with_window
    .filter(F.col("promotion_promo_code").isin(["WELCOME5", "VIP"]) & F.col("channel").isNotNull())
    .select(
        "*",
        F.round((F.col("promotion_discount_pct_3") * 0.85), 2).alias("adjusted_promotion_discount_pct_3"),
    )
)

totals = (
    base_promotions
    .groupBy(
        F.date_trunc("WEEK", "starts_on_minus_9d").alias("week_start"),
        "channel",
        F.date_trunc("QUARTER", "starts_on").alias("quarter_start"),
    )
    .agg(
        F.round(F.avg("promotion_discount_pct"), 2).alias("avg_promotion_discount_pct"),
        F.count(F.when(F.col("promotion_promo_code_3") == "SPRING10", 1)).alias("spring10_count"),
    )
    .filter(F.col("spring10_count") >= 1)
    .withColumn("avg_promotion_discount_pct_percent_rank", F.percent_rank().over(w))
)

result = (
    totals
    .select(
        "week_start",
        "channel",
        "quarter_start",
        "avg_promotion_discount_pct",
        "spring10_count",
    )
    .orderBy(F.col("spring10_count").asc_nulls_last())
)
