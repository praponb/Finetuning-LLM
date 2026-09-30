from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_orders
    .select(
        "id",
        "status",
        "order_date",
        "shipping_fee",
        F.last("shipping_fee").over(w).alias("last_shipping_fee"),
    )
)
