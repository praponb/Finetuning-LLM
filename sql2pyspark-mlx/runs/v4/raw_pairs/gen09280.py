from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("unit_price")
w3 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .withColumn("quantity_avg_6", F.avg("quantity").over(w))
    .withColumn("unit_price_cume_dist", F.cume_dist().over(w2))
    .withColumn("last_known_unit_price", F.last("unit_price", ignorenulls=True).over(w3))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w4))
)
