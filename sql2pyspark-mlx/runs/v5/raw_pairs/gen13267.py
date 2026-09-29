from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("store_id")

base = (
    df_orders
    .filter((F.col("shipping_fee") <= 18) & (F.col("order_date") >= F.lit("2023-06-03").cast("date")))
    .withColumn("discount_plus_shipping_fee", (F.col("discount") + F.col("shipping_fee")))
)

result = (
    base
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.avg("discount_plus_shipping_fee").over(w2).alias("store_id_avg_discount_plus_shipping_fee"),
        F.corr("discount", "shipping_fee").over(w2).alias("corr_discount_shipping_fee"),
    )
)
