from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_order_items
    .filter(F.col("order_id") == 32)
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.round(F.avg("unit_price").over(w), 2).alias("unit_price_moving_avg_3"),
    )
    .orderBy("unit_price")
)
