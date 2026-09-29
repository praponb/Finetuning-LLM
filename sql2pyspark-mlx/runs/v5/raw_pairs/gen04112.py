from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w_grp = Window.partitionBy("store_id").orderBy("shipping_fee", "id")
w_grp_2 = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "discount",
        "shipping_fee",
        F.corr("shipping_fee", "amount").over(w).alias("corr_shipping_fee_amount"),
        F.lag("shipping_fee", 1).over(w_grp).alias("prev_shipping_fee"),
        F.last("shipping_fee", ignorenulls=True).over(w_grp_2).alias("last_known_shipping_fee"),
    )
)
