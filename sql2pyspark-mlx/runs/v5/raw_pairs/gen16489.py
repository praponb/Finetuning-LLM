from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "order_date",
        "discount",
        F.round(F.avg("discount").over(w), 2).alias("discount_moving_avg_3"),
    )
)
