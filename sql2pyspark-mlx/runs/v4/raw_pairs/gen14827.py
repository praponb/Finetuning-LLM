from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w_running = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .withColumn("line_total", (F.col("quantity") + F.col("unit_price")))
)

result = (
    valid
    .withColumn("quantity_share", F.round((F.col("quantity") / F.sum("quantity").over(w)), 4))
    .withColumn("first_known_line_total", F.first("line_total", ignorenulls=True).over(w_running))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w_part))
)
