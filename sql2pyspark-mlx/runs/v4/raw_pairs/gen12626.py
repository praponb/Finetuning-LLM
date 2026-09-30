from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_orders
    .select(
        "id",
        "status",
        "shipping_fee",
        F.avg("shipping_fee").over(w).alias("status_avg_shipping_fee"),
    )
)
