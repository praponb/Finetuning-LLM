from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "order_date",
        "shipping_fee",
        F.lead("shipping_fee", 2).over(w).alias("next_shipping_fee"),
    )
    .orderBy("id")
)
