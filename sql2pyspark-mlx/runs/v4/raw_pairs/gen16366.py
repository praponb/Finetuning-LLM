from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("discount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 27)
    .select(
        "id",
        "channel",
        "order_date",
        "discount",
        "customer_id",
        "amount",
        F.lead("discount", 1).over(w).alias("next_discount"),
        F.last("amount").over(w2).alias("last_amount"),
    )
)
