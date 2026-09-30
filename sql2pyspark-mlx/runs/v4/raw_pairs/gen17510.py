from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id").orderBy("cart_value", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("created_at", "id").rowsBetween(-2, Window.currentRow)
w_running = Window.orderBy(F.col("cart_value").desc(), "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("cart_value").desc_nulls_last())
w_grp_2 = Window.partitionBy("ship_city", "customer_id").orderBy("cart_value", "id")

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "attributes",
        F.col("ship_to.city").alias("ship_city"),
        F.aggregate("items", F.lit(0.0), lambda acc, li: acc + li["qty"] * li["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.sum("cart_value").over(w_grp).alias("cumulative_cart_value"),
        F.avg("cart_value").over(w2).alias("cart_value_avg_2"),
        F.ntile(5).over(w_running).alias("cart_value_bucket"),
        F.percent_rank().over(w4).alias("cart_value_percent_rank"),
        F.row_number().over(w_grp_2).alias("seq_no"),
    )
)
