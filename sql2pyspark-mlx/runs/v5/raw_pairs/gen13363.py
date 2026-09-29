from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.hour("created_at").between(12, 15))
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
    )
)
