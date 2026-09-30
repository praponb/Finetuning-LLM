from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id")
w3 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_inventory
    .withColumn("running_quantity", F.sum("quantity").over(w))
    .withColumn("quantity_bucket", F.ntile(3).over(w2))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w3))
    .withColumn("quantity_bucket_2", F.ntile(3).over(w4))
)
