from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "items",
        "ship_to",
        F.col("attributes").getItem("device").alias("device"),
        F.transform(
            F.filter("items", lambda i: i["promo"].isNotNull()),
            lambda i: F.struct(i["product_id"].alias("pid"), F.round(i["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.array_contains(F.map_keys("attributes"), "gift").alias("has_gift"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
    )
)
