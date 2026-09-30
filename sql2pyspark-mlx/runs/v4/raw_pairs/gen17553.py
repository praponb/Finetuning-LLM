from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.orderBy("amount", "id")
w3 = Window.partitionBy("customer_id").orderBy("order_date", "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")
w5 = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc(), "id")

filtered = (
    df_orders
    .filter(F.col("order_date") < "2025-01-01")
    .withColumn("status_title", F.initcap(F.trim("status")))
)

result = (
    filtered
    .select(
        "*",
        F.corr("shipping_fee", "discount").over(w).alias("corr_shipping_fee_discount"),
        F.row_number().over(w2).alias("row_seq"),
        F.lag("shipping_fee", 1).over(w3).alias("prev_shipping_fee"),
        F.ntile(5).over(w4).alias("shipping_fee_bucket"),
        F.ntile(3).over(w5).alias("shipping_fee_bucket_2"),
    )
)
