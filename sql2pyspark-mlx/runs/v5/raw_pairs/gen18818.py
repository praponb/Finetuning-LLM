from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())
w3 = Window.orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w))
    .withColumn("quantity_cume_dist", F.cume_dist().over(w2))
    .withColumn("quantity_cume_dist_2", F.cume_dist().over(w3))
)
