from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id")

result = (
    df_inventory
    .withColumn("quantity_bucket", F.ntile(4).over(w))
    .withColumn("store_id_avg_quantity", F.avg("quantity").over(w2))
)
