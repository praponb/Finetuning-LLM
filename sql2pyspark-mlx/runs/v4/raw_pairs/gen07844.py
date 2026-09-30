from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .withColumn("quantity_bucket", F.ntile(10).over(w))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w2))
)
