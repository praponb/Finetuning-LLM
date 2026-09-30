from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount", "id")
w2 = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")
w3 = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc(), "id")

metrics = (
    df_orders
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "shipping_fee",
        F.lag("discount", 1).over(w).alias("prev_discount"),
        F.ntile(3).over(w2).alias("shipping_fee_bucket"),
        F.ntile(10).over(w3).alias("shipping_fee_bucket_2"),
    )
)

result = (
    metrics
    .select("id", "order_date", "shipping_fee", "prev_discount", "shipping_fee_bucket")
    .orderBy(F.col("shipping_fee_bucket").desc_nulls_last(), "id")
)
