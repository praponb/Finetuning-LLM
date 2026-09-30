from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("status", "customer_id")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "discount",
        "status",
        "customer_id",
        F.max("discount").over(w).alias("store_id_max_discount"),
        F.sum("discount").over(w2).alias("status_sum_discount"),
    )
)
