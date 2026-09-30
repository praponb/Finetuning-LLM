from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("discount", "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")
w4 = Window.orderBy(F.col("discount").desc(), "id")
w5 = Window.partitionBy("channel", "store_id").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "order_date",
        "channel",
        "discount",
        F.avg("shipping_fee").over(w).alias("shipping_fee_avg_5"),
        F.lead("discount", 1, 0).over(w2).alias("next_discount"),
        F.row_number().over(w3).alias("row_seq"),
        F.ntile(4).over(w4).alias("discount_bucket"),
        F.row_number().over(w5).alias("event_seq"),
    )
)
