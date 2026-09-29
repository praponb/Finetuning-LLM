from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id").orderBy("launch_date", "id")
w_hist = Window.orderBy("price", "id")
w3 = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("supplier_id", "category")
w5 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("prev_price", F.lag("price", 1).over(w_hist))
    .withColumn("running_stock_qty", F.sum("stock_qty").over(w3))
    .withColumn("supplier_id_max_stock_qty", F.max("stock_qty").over(w4))
    .withColumn("first_known_price", F.first("price", ignorenulls=True).over(w5))
)
