from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("rec", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("rec.qty"),
        F.col("rec.price"),
        F.col("rec.promo"),
    )
)

lines = (
    flattened
    .select("created_at", "qty", "price", F.explode("coupon_codes").alias("code"))
)

cleaned = (
    lines
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-01"))) & (F.col("qty") >= 5))
    .select(
        "*",
        F.col("created_at").cast("date").alias("created_at_day"),
        F.to_date("created_at").alias("created_at_date"),
    )
)

base = (
    cleaned
    .filter(F.col("code").isNotNull())
    .select(
        "qty",
        "price",
        "code",
        "created_at_day",
        "created_at_date",
        F.date_format("created_at_date", "yyyy-MM-dd").alias("created_at_date_label"),
        F.length("code").alias("code_length"),
        F.coalesce("code", F.lit("N/A")).alias("code_clean"),
    )
)

result = (
    base
    .select(
        "code",
        "created_at_day",
        "created_at_date",
        "created_at_date_label",
        F.when(F.col("code").rlike("^(SPRING10|VIP)$"), "EXTENDED").when(F.col("code").rlike("^(BUNDLE|WELCOME5)$"), "GROUP_A").otherwise("PRIORITY").alias("code_zone"),
    )
)
