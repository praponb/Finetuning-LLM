from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "items",
        "ship_to",
        F.col("attributes")["gift"].alias("gift"),
        F.size("attributes").alias("num_attributes"),
        F.transform(
            F.filter("items", lambda i: i["promo"].isNotNull()),
            lambda i: F.struct(i["product_id"].alias("pid"), F.round(i["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
    )
)
