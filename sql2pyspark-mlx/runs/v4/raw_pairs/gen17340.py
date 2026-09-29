from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w2 = Window.orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        (F.col("unit_price") - F.lag("unit_price", 2, 0).over(w)).alias("unit_price_change"),
        F.round(F.avg("quantity").over(w2), 2).alias("quantity_moving_avg_7"),
    )
    .orderBy("quantity_moving_avg_7", F.col("id").desc())
)
