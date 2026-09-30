from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "id",
        "coupon_codes",
        "items",
        "ship_to",
        F.size("items").alias("num_items"),
        F.col("ship_to.country").alias("ship_country"),
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
    )
)
