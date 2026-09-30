from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("category", "brand").orderBy(F.col("stock_qty").asc_nulls_first())
w_grp = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc(), "id")

result = (
    df_products
    .withColumn("last_known_stock_qty", F.last("stock_qty", ignorenulls=True).over(w_part))
    .withColumn("stock_qty_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w_grp))
)
