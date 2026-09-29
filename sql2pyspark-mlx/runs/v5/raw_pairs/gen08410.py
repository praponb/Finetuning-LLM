from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("channel").orderBy("discount", "id")

result = (
    df_orders
    .filter(F.col("amount").isNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        F.ntile(5).over(w_ordered).alias("shipping_fee_bucket"),
        F.ntile(10).over(w2).alias("discount_bucket"),
    )
)
