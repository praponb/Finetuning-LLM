from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("ship_country").orderBy("cart_value", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("ship_city").orderBy("cart_value", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_carts
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "id",
        "coupon_codes",
        "ship_to",
        F.col("ship_to.city").alias("ship_city"),
        F.col("ship_to.country").alias("ship_country"),
        F.aggregate("items", F.lit(0.0), lambda acc, li: acc + li["qty"] * li["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.col("attributes").getItem("device").alias("device"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.min("cart_value").over(w_part).alias("running_min_cart_value"),
        F.sum("cart_value").over(w_part).alias("running_sum_cart_value"),
        F.count("cart_value").over(w_part).alias("running_count_cart_value"),
        F.sum("cart_value").over(w2).alias("running_cart_value"),
    )
)
