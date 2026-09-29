from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2024-03-11").cast("date")) & F.col("channel").isNotNull())
    .select(
        "*",
        F.concat_ws("/", "promo_code", "channel").alias("promo_code_channel_key"),
    )
)

matched = (
    filtered
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

attributed = (
    matched
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

attributed_promotions = (
    attributed
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

result = (
    attributed_promotions
    .groupBy(
        F.date_trunc("week", "starts_on").alias("week_start"),
        "promotion_promo_code_3",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count_if(F.col("promotion_promo_code_3") == "VIP").alias("vip_events"),
        F.count(F.when(F.col("promotion_promo_code_2") == "SPRING10", 1)).alias("spring10_rows"),
    )
    .filter(F.col("spring10_rows") < 1)
    .orderBy("spring10_rows")
)
