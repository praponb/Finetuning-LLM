from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("basket_amount", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy(F.col("basket_amount").desc(), "id")
w3 = Window.orderBy("basket_amount", "id").rowsBetween(-5, -1)

parsed = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.col("ship_to.zip").alias("ship_zip"),
        F.array_contains("coupon_codes", "WELCOME5").alias("used_welcome5"),
        F.aggregate("items", F.lit(0.0), lambda acc, li: acc + li["qty"] * li["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
    )
)

result = (
    parsed
    .withColumn("basket_amount_avg_6", F.avg("basket_amount").over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("basket_amount_avg_5_prior", F.avg("basket_amount").over(w3))
)
