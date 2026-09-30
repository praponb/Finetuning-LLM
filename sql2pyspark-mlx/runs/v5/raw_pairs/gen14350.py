from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_order_items
    .filter(F.col("product_id") == 22)
    .select(
        "id",
        "order_id",
        "quantity",
        F.round(F.avg("quantity").over(w), 2).alias("quantity_moving_avg_3"),
    )
    .orderBy("quantity")
)
