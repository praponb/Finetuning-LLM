from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .withColumn("cumulative_unit_price", F.sum("unit_price").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)
