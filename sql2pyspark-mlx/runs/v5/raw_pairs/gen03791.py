from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-11-06").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_add("ends_on", 66).alias("ends_on_plus_66d"),
        F.round((F.col("discount_pct") * 100), 2).alias("discount_pct_adjusted"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

result = (
    valid
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & F.col("src.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)
