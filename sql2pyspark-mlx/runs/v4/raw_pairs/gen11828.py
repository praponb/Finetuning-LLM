from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 24)
    .select(
        "id",
        "customer_id",
        "order_date",
        "discount",
        F.round(F.avg("discount").over(w), 2).alias("discount_moving_avg_7"),
    )
)
