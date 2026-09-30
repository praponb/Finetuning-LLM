from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "order_date",
        "discount",
        F.round(F.avg("discount").over(w), 2).alias("discount_moving_avg_7"),
    )
    .orderBy("id", F.col("discount").desc())
)
