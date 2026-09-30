from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .filter(F.col("store_id") == 3)
    .withColumn("running_min_quantity", F.min("quantity").over(w))
    .withColumn("running_sum_quantity", F.sum("quantity").over(w))
    .withColumn("running_avg_quantity", F.avg("quantity").over(w))
    .withColumn("running_max_quantity", F.max("quantity").over(w))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w2))
)
