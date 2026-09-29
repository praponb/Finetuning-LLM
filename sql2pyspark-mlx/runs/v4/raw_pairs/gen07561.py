from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "attributes",
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.size("items").alias("num_items"),
        F.transform(
            F.filter("items", lambda i: i["promo"].isNotNull()),
            lambda i: F.struct(i["product_id"].alias("pid"), F.round(i["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
    )
)
