from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.coalesce(F.col("attributes")["device"], F.lit("direct")).alias("device_or_default"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
    )
)
