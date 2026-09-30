from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("basket_amount", "id")
w2 = Window.partitionBy("customer_id").orderBy("basket_amount")
w3 = Window.orderBy("basket_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "ship_to",
        F.coalesce(F.col("attributes")["utm_source"], F.lit("unknown")).alias("utm_source_or_default"),
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.coalesce(F.col("attributes").getItem("device"), F.lit("direct")).alias("device_or_default"),
    )
)

result = (
    parsed
    .select(
        "id",
        "customer_id",
        "utm_source_or_default",
        "basket_amount",
        F.lag("basket_amount", 2, 0).over(w).alias("prev_basket_amount"),
        F.cume_dist().over(w2).alias("basket_amount_cume_dist"),
        F.sum("basket_amount").over(w3).alias("cumulative_basket_amount"),
        F.sum("basket_amount").over(w3).alias("cumulative_basket_amount_2"),
    )
)
