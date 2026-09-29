from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel")

result = (
    df_orders
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.sum("discount").over(w2).alias("cumulative_discount"),
        F.last("channel", ignorenulls=True).over(w3).alias("last_known_channel"),
        F.corr("discount", "shipping_fee").over(w4).alias("corr_discount_shipping_fee"),
    )
)
