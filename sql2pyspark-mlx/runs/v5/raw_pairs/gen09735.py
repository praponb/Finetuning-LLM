from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.coalesce(F.col("attributes")["device"], F.lit("direct")).alias("device_or_default"),
    )
)
