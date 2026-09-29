from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("ends_on") >= F.to_date(F.lit("2023-08-31"))))
    .select(
        "*",
        F.when(F.col("discount_pct") >= 0.35, "High").when(F.col("discount_pct") >= 0.15, "Medium").otherwise("Low").alias("discount_pct_band"),
        F.trim("channel").alias("clean_channel"),
    )
)

cleaned = (
    valid
    .filter(F.col("channel").isNotNull() & (F.col("discount_pct") <= 0.29))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "discount_pct_band",
        "clean_channel",
        F.year("starts_on").alias("starts_on_year"),
        F.when(F.col("channel").isin(["partner", "store"]), "Priority").otherwise("Other").alias("channel_group"),
    )
)

attributed = (
    cleaned
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

base = (
    attributed
    .filter(F.col("starts_on") >= F.lit("2023-07-17").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "discount_pct_band",
        "clean_channel",
        "starts_on_year",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        F.round("starts_on_year", 1).alias("starts_on_year_rounded"),
        F.round((F.col("starts_on_year") * 1.15), 2).alias("starts_on_year_adjusted"),
        F.coalesce("promotion_promo_code", F.lit("OTHER")).alias("resolved_promotion_promo_code"),
    )
)

base_promotions = (
    base
    .filter((F.col("promotion_promo_code").eqNullSafe(F.lit("SPRING10"))) & F.col("promotion_promo_code").isNotNull())
    .withColumn("adjusted_starts_on_year_adjusted", F.round((F.col("starts_on_year_adjusted") * 1.15), 2))
)

result = (
    base_promotions
    .select(
        "id",
        "promo_id",
        "promo_code",
        F.col("adjusted_starts_on_year_adjusted").alias("ADJUSTED_STARTS_ON_YEAR_ADJUSTED"),
        F.col("starts_on_year_rounded").alias("STARTS_ON_YEAR_ROUNDED"),
        "starts_on_year_adjusted",
        F.col("discount_pct").alias("DISCOUNT_PCT_VAL"),
    )
    .unpivot(
        ids=["id", "promo_id", "promo_code"],
        values=["ADJUSTED_STARTS_ON_YEAR_ADJUSTED", "STARTS_ON_YEAR_ROUNDED", "starts_on_year_adjusted", "DISCOUNT_PCT_VAL"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
)
