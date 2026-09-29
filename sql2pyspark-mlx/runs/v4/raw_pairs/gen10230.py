from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

lines = (
    flattened
    .select(
        "id",
        "created_at",
        "qty",
        "price",
        "promo",
        F.explode("coupon_codes").alias("code"),
    )
)

base = (
    lines
    .filter((F.col("created_at") > F.to_timestamp(F.lit("2025-03-02 16:00:00"))) & F.col("code").isNotNull() & (F.to_date("created_at") < "2023-08-08"))
    .withColumn("promo_code", F.concat("promo", F.lit(" "), "code"))
    .withColumn("resolved_code", F.coalesce("code", F.lit("DIRECT")))
)

base_carts = (
    base
    .filter(F.to_date("created_at") < "2023-03-01")
    .select(
        "*",
        F.col("created_at").cast("date").alias("created_at_day"),
        F.round((F.col("qty") * 1.1), 2).alias("adjusted_qty"),
    )
)

result = (
    base_carts
    .select(
        "id",
        "created_at",
        "qty",
        "price",
        "promo",
        "code",
        "resolved_code",
        "adjusted_qty",
    )
)
