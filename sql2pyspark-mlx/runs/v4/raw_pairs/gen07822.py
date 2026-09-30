from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .withColumn("next_quantity", F.lead("quantity", 1).over(w))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w2))
    .withColumn("running_quantity", F.sum("quantity").over(w3))
)
