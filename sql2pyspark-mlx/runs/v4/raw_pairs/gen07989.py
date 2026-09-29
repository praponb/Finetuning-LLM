from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "status").orderBy("amount", "id")
w2 = Window.partitionBy("channel").orderBy("order_date", "id")
w3 = Window.partitionBy("status").orderBy("amount", "id")

result = (
    df_orders
    .select(
        "*",
        F.lag("discount", 1).over(w).alias("prev_discount"),
        F.lead("shipping_fee", 1).over(w2).alias("next_shipping_fee"),
        F.ntile(5).over(w3).alias("amount_bucket"),
    )
)
