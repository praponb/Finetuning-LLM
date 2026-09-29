from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.promo"),
    )
)

lines = (
    exploded
    .select("id", "created_at", "promo", F.explode_outer("coupon_codes").alias("code"))
)

filtered = (
    lines
    .filter(F.col("code").eqNullSafe(F.lit("VIP")))
    .withColumn("created_at_label", F.date_format("created_at", "MMM yyyy"))
    .withColumn("promo_filled", F.coalesce("promo", F.lit("OTHER")))
)

cleaned = (
    filtered
    .filter((F.col("created_at") >= F.lit("2025-03-01 22:00:00").cast("timestamp")) & (F.col("created_at") >= "2023-04-17"))
    .select(
        "id",
        "created_at",
        "code",
        "created_at_label",
        "promo_filled",
        F.to_date("created_at").alias("created_at_date"),
        F.coalesce("code", F.lit("DIRECT")).alias("code_filled"),
    )
)

result = (
    cleaned
    .select(
        "code",
        "promo_filled",
        "code_filled",
        F.when(F.col("code").rlike("^(WELCOME5|BUNDLE)$"), "CORE").otherwise("PRIORITY").alias("code_zone"),
    )
    .orderBy("code_zone", "code")
)
