from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "customer_id").orderBy("shipping_fee", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("channel", "store_id").orderBy("discount", "id")
w3 = Window.orderBy(F.col("amount").desc(), "id")
w4 = Window.orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .select(
        "*",
        F.avg("shipping_fee").over(w).alias("shipping_fee_avg_2_prior"),
        F.lag("shipping_fee", 1).over(w2).alias("prev_shipping_fee"),
        F.row_number().over(w3).alias("seq_no"),
        F.ntile(5).over(w4).alias("discount_bucket"),
    )
)
