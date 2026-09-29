from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-09-01"))) & F.col("channel").isNotNull())
    .select(
        "*",
        F.round("discount_pct", 2).alias("discount_pct_rounded"),
        F.floor("discount_pct").alias("discount_pct_whole"),
    )
)

cleaned = (
    filtered
    .filter(F.col("discount_pct_rounded").between(35, 39))
    .select(
        "*",
        F.coalesce("discount_pct_rounded", F.lit(0)).alias("discount_pct_rounded_filled"),
        F.round((F.col("discount_pct") * 1.1), 2).alias("discount_pct_adjusted"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

with_window = (
    cleaned
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

attributed = (
    with_window
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

valid = (
    attributed
    .filter((F.col("promotion_promo_code") == "WELCOME5") & (F.col("starts_on") >= F.lit("2023-06-05").cast("date")))
    .withColumn("promotion_discount_pct_level", F.when(F.col("promotion_discount_pct") >= 0.27, "High").when(F.col("promotion_discount_pct") >= 0.16, "Medium").otherwise("Low"))
)

result = (
    valid
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        "discount_pct_rounded",
        "discount_pct_whole",
        "discount_pct_rounded_filled",
        "resolved_channel",
        "promo_id",
    )
)
