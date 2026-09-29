from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .withColumn("order_id_avg_quantity", F.avg("quantity").over(w))
    .withColumn("order_id_min_quantity", F.min("quantity").over(w))
    .withColumn("last_known_unit_price", F.last("unit_price", ignorenulls=True).over(w2))
)
