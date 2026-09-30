from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id")
w2 = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w2))
)
