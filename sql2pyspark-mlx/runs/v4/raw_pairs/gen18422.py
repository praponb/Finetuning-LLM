from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "shipping_fee",
        F.max("shipping_fee").over(w).alias("store_id_max_shipping_fee"),
    )
    .orderBy(F.col("store_id_max_shipping_fee").desc())
)
