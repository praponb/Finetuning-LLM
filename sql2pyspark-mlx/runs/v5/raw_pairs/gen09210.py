from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.size("coupon_codes") > 1)
    .select(
        "id",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
    )
)
