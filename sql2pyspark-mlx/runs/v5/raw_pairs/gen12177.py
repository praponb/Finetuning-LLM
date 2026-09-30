from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "ship_to",
        F.size(F.filter("items", lambda i: i["qty"] >= 3)).alias("bulk_lines"),
        F.col("attributes")["gift"].alias("gift"),
        F.coalesce(F.col("attributes")["device"], F.lit("unknown")).alias("device_or_default"),
    )
)
