from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .withColumn("quantity_avg_3", F.avg("quantity").over(w))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w2))
)
