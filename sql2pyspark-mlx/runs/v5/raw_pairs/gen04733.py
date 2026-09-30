from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("created_at") >= "2024-01-01")
    .select(
        "id",
        "created_at",
        F.transform(
            F.filter("items", lambda item: item["promo"].isNotNull()),
            lambda item: F.struct(item["product_id"].alias("pid"), F.round(item["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
        F.size("attributes").alias("num_attributes"),
        F.coalesce(F.col("attributes")["device"], F.lit("none")).alias("device_or_default"),
    )
)
