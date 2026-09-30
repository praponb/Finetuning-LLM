from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "unit_price",
        F.round(F.avg("unit_price").over(w), 2).alias("unit_price_moving_avg_7"),
    )
)
