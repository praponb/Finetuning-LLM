from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "attributes",
        F.coalesce(F.col("attributes")["gift"], F.lit("direct")).alias("gift_or_default"),
        F.col("attributes")["gift"].alias("gift"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.size(F.filter("items", lambda x: x["qty"] >= 3)).alias("bulk_lines"),
    )
)
