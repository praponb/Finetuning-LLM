from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("quantity", "id")

result = (
    df_order_items
    .withColumn("last3_avg_quantity", F.avg("quantity").over(w))
    .withColumn("last3_count_quantity", F.count("quantity").over(w))
    .withColumn("last3_max_quantity", F.max("quantity").over(w))
    .withColumn("quantity_bucket", F.ntile(3).over(w2))
)
