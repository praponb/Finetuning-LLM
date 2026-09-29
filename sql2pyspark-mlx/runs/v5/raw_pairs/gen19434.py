from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-05-02"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.add_months("ends_on", 1).alias("ends_on_plus_months"),
        F.last_day("ends_on").alias("ends_on_month_end"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

result = (
    cleaned
    .select("promo_code")
    .subtract(
        df_promotions
        .filter(F.col("discount_pct") < 0.15)
        .select("promo_code")
    )
)
