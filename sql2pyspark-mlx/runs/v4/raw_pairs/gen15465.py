from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id")
w3 = Window.partitionBy("brand").orderBy("launch_date", "id")
w4 = Window.partitionBy("brand")
w5 = Window.partitionBy("supplier_id", "category").orderBy("stock_qty", "id")

result = (
    df_products
    .withColumn("cumulative_cost", F.sum("cost").over(w))
    .withColumn("supplier_id_stddev_samp_price", F.stddev_samp("price").over(w2))
    .withColumn("prev_stock_qty", F.lag("stock_qty", 1, 0).over(w3))
    .withColumn("brand_count_cost", F.count("cost").over(w4))
    .withColumn("next_cost", F.lead("cost", 1, 0).over(w5))
)
