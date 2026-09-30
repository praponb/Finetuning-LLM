from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
        F.array_contains(F.map_keys("attributes"), "gift").alias("has_gift"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.size("attributes").alias("num_attributes"),
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
    )
)
