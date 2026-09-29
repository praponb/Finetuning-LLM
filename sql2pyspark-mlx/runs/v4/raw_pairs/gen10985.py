from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2024-06-05").cast("date")) & (F.col("promo_code") != "BUNDLE"))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        F.weekofyear("ends_on").alias("ends_on_weekofyear"),
        F.when(F.col("channel").isin(["mobile", "store"]), "Group A").otherwise("Other").alias("channel_group"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

matched = (
    cleaned
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("s.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

matched_promotions = (
    matched
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

base = (
    matched_promotions
    .filter(F.col("promotion_promo_code").isNotNull())
    .withColumn("promo_id_times_discount_pct", (F.col("promo_id") * F.col("discount_pct")))
)

wide = (
    base
    .groupBy("channel")
    .pivot("promo_code", ["BUNDLE", "SPRING10", "WELCOME5"])
    .agg(F.max("promo_id"))
)

result = (
    wide
    .select("channel", "BUNDLE", "SPRING10", "WELCOME5")
    .orderBy(F.col("BUNDLE").desc())
)
