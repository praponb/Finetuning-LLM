from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("last_restock", "id")

result = (
    df_inventory
    .withColumn("cumulative_quantity", F.sum("quantity").over(w))
    .withColumn("next_quantity", F.lead("quantity", 1).over(w2))
)
