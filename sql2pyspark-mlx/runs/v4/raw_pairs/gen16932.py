from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_order_items
    .withColumn("product_id_count_quantity", F.count("quantity").over(w))
    .withColumn("cumulative_unit_price", F.sum("unit_price").over(w2))
    .withColumn("unit_price_avg_2", F.avg("unit_price").over(w3))
)
