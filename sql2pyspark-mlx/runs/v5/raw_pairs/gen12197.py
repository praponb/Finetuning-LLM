from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("discount", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("shipping_fee", "id")
w3 = Window.orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "amount",
        "shipping_fee",
        F.avg("shipping_fee").over(w).alias("shipping_fee_avg_5"),
        F.ntile(4).over(w2).alias("shipping_fee_bucket"),
        F.row_number().over(w3).alias("row_seq"),
    )
)
