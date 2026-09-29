from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_inventory
    .withColumn("running_avg_quantity", F.avg("quantity").over(w))
    .withColumn("running_count_quantity", F.count("quantity").over(w))
    .withColumn("running_sum_quantity", F.sum("quantity").over(w))
    .withColumn("quantity_bucket", F.ntile(5).over(w2))
)
