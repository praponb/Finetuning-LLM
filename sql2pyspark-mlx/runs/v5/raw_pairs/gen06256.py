from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id", "customer_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.sum("shipping_fee").over(w_hist).alias("running_sum_shipping_fee"),
        F.min("shipping_fee").over(w_hist).alias("running_min_shipping_fee"),
        F.count("shipping_fee").over(w_hist).alias("running_count_shipping_fee"),
    )
)
