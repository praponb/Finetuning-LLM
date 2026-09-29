from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipping_fee", "id")
w_running = Window.partitionBy("store_id")
w3 = Window.partitionBy("status").orderBy(F.col("discount").desc_nulls_last())

with_windows = (
    df_orders
    .withColumn("next_shipping_fee", F.lead("shipping_fee", 2, 0).over(w))
    .withColumn("corr_shipping_fee_amount", F.corr("shipping_fee", "amount").over(w_running))
    .withColumn("discount_rank", F.rank().over(w3))
)

result = (
    with_windows
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        "next_shipping_fee",
        "corr_shipping_fee_amount",
    )
    .orderBy(F.col("store_id").desc())
)
