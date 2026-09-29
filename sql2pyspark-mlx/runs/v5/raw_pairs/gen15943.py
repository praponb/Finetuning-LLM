from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.to_date("created_at") >= "2025-10-01")
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        "attributes",
        F.exists("items", lambda item: item["price"] > 100).alias("has_premium_item"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("unknown")).alias("utm_source_or_default"),
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
    )
)
