from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")
w2 = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_orders
    .filter(~F.col("status").isin(["CANCELLED", "SHIPPED"]))
    .select(
        "id",
        "customer_id",
        "order_date",
        "discount",
        "status",
        "shipping_fee",
        F.sum("discount").over(w).alias("running_discount"),
        F.round(F.avg("shipping_fee").over(w2), 2).alias("shipping_fee_moving_avg_7"),
    )
)
