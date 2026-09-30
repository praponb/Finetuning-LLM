from pyspark.sql import functions as F

base = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-09-12").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.when(F.col("promo_code") == "VIP", 1).otherwise(0).alias("is_vip"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

result = (
    base
    .groupBy("promo_code")
    .pivot("channel", ["store", "partner", "mobile"])
    .agg(F.sum("discount_pct"))
)
