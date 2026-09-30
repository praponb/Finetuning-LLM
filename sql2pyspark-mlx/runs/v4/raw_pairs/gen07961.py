from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at", "id")
w_running = Window.partitionBy("customer_id").orderBy("basket_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("basket_amount", "id")
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("customer_id").orderBy("basket_amount", "id").rowsBetween(-3, -1)

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("ship_to.zip").alias("ship_zip"),
        F.aggregate("items", F.lit(0.0), lambda acc, li: acc + li["qty"] * li["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
        F.col("attributes").getItem("device").alias("device"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.lead("basket_amount", 2).over(w).alias("next_basket_amount"),
        F.sum("basket_amount").over(w_running).alias("cumulative_basket_amount"),
        F.row_number().over(w_part).alias("row_seq"),
        F.max("basket_amount").over(w4).alias("customer_id_max_basket_amount"),
        F.avg("basket_amount").over(w5).alias("basket_amount_avg_3_prior"),
    )
)
