from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")
w2 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_order_items
    .filter(F.col("unit_price") <= 140)
    .select(
        "id",
        "order_id",
        "product_id",
        F.ntile(5).over(w).alias("quantity_bucket"),
        F.sum("unit_price").over(w2).alias("running_unit_price"),
        F.avg("unit_price").over(w3).alias("unit_price_avg_2"),
    )
)
