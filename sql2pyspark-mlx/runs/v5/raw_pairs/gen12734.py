from pyspark.sql import functions as F

base = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_sub("starts_on", 11).alias("starts_on_minus_11d"),
    )
)

result = (
    base
    .select("channel", "promo_code")
    .intersect(
        df_promotions
        .filter(F.col("starts_on") >= "2024-12-19")
        .select("channel", "promo_code")
    )
)
