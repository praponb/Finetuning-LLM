from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id").orderBy("discount", "id")
w_hist = Window.partitionBy("store_id")

result = (
    df_orders
    .select(
        "id",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.ntile(3).over(w_grp).alias("discount_bucket"),
        F.count("shipping_fee").over(w_hist).alias("store_id_count_shipping_fee"),
    )
)
