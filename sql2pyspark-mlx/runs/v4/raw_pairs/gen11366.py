from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "status")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "status",
        "shipping_fee",
        F.avg("shipping_fee").over(w).alias("store_id_avg_shipping_fee"),
    )
)
