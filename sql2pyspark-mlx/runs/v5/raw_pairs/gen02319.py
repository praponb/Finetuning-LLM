from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.round(F.avg("quantity").over(w), 2).alias("quantity_moving_avg_7"),
    )
    .orderBy(F.col("quantity").desc())
)
