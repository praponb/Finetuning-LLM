from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")
w_grp = Window.partitionBy("brand", "supplier_id").orderBy("stock_qty")
w3 = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .withColumn("prev_stock_qty", F.lag("stock_qty", 2).over(w))
    .withColumn("stock_qty_rank", F.rank().over(w_grp))
    .withColumn("running_stock_qty", F.sum("stock_qty").over(w3))
)
