from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter(F.col("starts_on").between("2023-10-12", "2024-09-02"))
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct",
        F.upper("promo_code").alias("promo_code_upper"),
        F.datediff("ends_on", "starts_on").alias("days_starts_on_to_ends_on"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

result = (
    filtered
    .select("id", "channel")
    .intersect(
        df_promotions
        .filter(F.col("ends_on") > "2023-07-04")
        .select("id", "channel")
    )
)
