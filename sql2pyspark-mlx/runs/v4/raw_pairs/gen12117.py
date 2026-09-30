from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-12-20"))) & F.col("channel").isNotNull() & (F.col("ends_on") <= "2025-03-01"))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.datediff(F.current_date(), "starts_on").alias("days_since_starts_on"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

result = (
    valid
    .groupBy("promo_code")
    .pivot("channel", ["store", "web"])
    .agg(F.sum("discount_pct").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
