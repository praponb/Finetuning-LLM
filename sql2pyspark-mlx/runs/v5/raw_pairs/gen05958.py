from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("line_no"),
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.promo"),
    )
)

result = flattened.select("id", "customer_id", "line_no", "product_id", "qty", "promo")
