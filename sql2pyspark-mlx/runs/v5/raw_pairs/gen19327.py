from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("store_id", "customer_id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "discount",
        "store_id",
        F.avg("discount").over(w).alias("customer_id_avg_discount"),
        F.avg("discount").over(w2).alias("store_id_avg_discount"),
    )
)
