from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "brand")
w_ordered = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_products
    .withColumn("supplier_id_sum_price", F.sum("price").over(w))
    .withColumn("cost_avg_2", F.avg("cost").over(w_ordered))
)
