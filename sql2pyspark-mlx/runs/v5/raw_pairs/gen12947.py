from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-08-28").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.upper("channel").alias("channel_upper"),
    )
)

result = (
    cleaned
    .groupBy("promo_code")
    .pivot("channel", ["partner", "mobile"])
    .agg(F.max("discount_pct").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
