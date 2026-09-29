from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")
w3 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id").rowsBetween(-3, -1)
w5 = Window.orderBy("quantity", "id")

result = (
    df_inventory
    .withColumn("quantity_avg_5_prior", F.avg("quantity").over(w))
    .withColumn("next_quantity", F.lead("quantity", 1).over(w2))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w3))
    .withColumn("quantity_avg_3_prior", F.avg("quantity").over(w4))
    .withColumn("quantity_bucket", F.ntile(3).over(w5))
)
