from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .withColumn("first_known_unit_price", F.first("unit_price", ignorenulls=True).over(w))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w2))
)
