from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_orders
    .filter(F.col("shipping_fee") < 21)
    .select(
        "id",
        "channel",
        "order_date",
        "discount",
        "customer_id",
        "amount",
        F.sum("discount").over(w).alias("running_discount"),
        F.last("order_date").over(w2).alias("last_order_date"),
    )
    .orderBy(F.col("running_discount").desc(), "id")
)
