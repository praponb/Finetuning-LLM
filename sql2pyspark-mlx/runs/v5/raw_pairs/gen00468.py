from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("status", "customer_id").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "status",
        "shipping_fee",
        "customer_id",
        "order_date",
        F.row_number().over(w).alias("shipping_fee_row"),
        F.sum("shipping_fee").over(w2).alias("running_shipping_fee"),
    )
    .orderBy("order_date", "id")
)
