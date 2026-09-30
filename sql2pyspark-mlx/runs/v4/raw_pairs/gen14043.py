from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w2 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("running_unit_price", F.sum("unit_price").over(w2))
)
