from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")
w2 = Window.partitionBy("store_id").orderBy("shipping_fee", "id")

result = (
    df_orders
    .filter(F.col("discount") > 0.15)
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "store_id",
        "shipping_fee",
        F.first("channel").over(w).alias("first_channel"),
        F.row_number().over(w2).alias("rn"),
    )
)
