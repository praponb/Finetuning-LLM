from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-08-14").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.year("starts_on").alias("starts_on_year"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

attributed = (
    filtered
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & F.col("a.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

filtered_promotions = (
    attributed
    .filter(F.col("channel").isNotNull() & (F.col("ends_on") >= F.lit("2023-12-13").cast("date")))
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "starts_on_year",
        "resolved_channel",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        (F.col("starts_on_year") - F.col("promo_id")).alias("starts_on_year_minus_promo_id"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

valid = (
    filtered_promotions
    .filter(F.col("promotion_promo_code").isNotNull() & (F.col("promo_id") <= 8))
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "starts_on_year",
        "resolved_channel",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "starts_on_year_minus_promo_id",
        "channel_filled",
        F.col("starts_on_year_minus_promo_id").cast("int").alias("starts_on_year_minus_promo_id_int"),
    )
)

result = (
    valid
    .groupBy(
        "promo_id",
        F.date_trunc("week", "ends_on").alias("week_start"),
        "promotion_promo_code",
    )
    .agg(
        F.count_if(F.col("promotion_promo_code") == "BUNDLE").alias("bundle_events"),
        F.count(F.when(F.col("promotion_promo_code") == "WELCOME5", 1)).alias("welcome5_count"),
        F.count(F.when(F.col("promotion_promo_code") == "WELCOME5", 1)).alias("welcome5_rows"),
    )
)
