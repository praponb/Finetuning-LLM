from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("order_id")
w2 = Window.partitionBy("product_id")
w_grp = Window.partitionBy("order_id").orderBy("unit_price", "id")
w4 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_order_items
    .withColumn("order_id_avg_unit_price", F.avg("unit_price").over(w_running))
    .withColumn("quantity_share", F.round((F.col("quantity") / F.sum("quantity").over(w2)), 4))
    .withColumn("row_seq", F.row_number().over(w_grp))
    .withColumn("cumulative_unit_price", F.sum("unit_price").over(w4))
    .withColumn("row_seq_2", F.row_number().over(w5))
)
