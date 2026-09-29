from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("channel").orderBy("shipping_fee", "id")

result = (
    df_orders
    .filter(F.col("status") == "CANCELLED")
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "discount",
        F.lead("shipping_fee").over(w_recent).alias("next_shipping_fee"),
        F.lag("shipping_fee").over(w_recent).alias("prev_shipping_fee"),
        F.lead("discount").over(w_recent).alias("next_discount"),
        F.lag("discount").over(w_recent).alias("prev_discount"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.ntile(3).over(w2).alias("shipping_fee_bucket"),
    )
)
