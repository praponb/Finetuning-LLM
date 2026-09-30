from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "ship_to",
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
        F.col("attributes")["gift"].alias("gift"),
        F.size(F.filter("items", lambda x: x["qty"] >= 3)).alias("bulk_lines"),
    )
)
