from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.to_date("created_at") >= "2024-09-05")
    .select(
        "id",
        "created_at",
        "ship_to",
        F.col("attributes")["device"].alias("device"),
        F.transform(
            F.filter("items", lambda li: li["promo"].isNotNull()),
            lambda li: F.struct(li["product_id"].alias("pid"), F.round(li["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.aggregate("items", F.lit(0.0), lambda acc, li: acc + li["qty"] * li["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
    )
)
