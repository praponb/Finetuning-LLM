from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "store_id").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        F.sum("discount").over(w).alias("running_discount"),
        F.ntile(5).over(w2).alias("shipping_fee_bucket"),
    )
)
