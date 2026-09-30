from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)
w_ordered = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())
w3 = Window.partitionBy("store_id").orderBy("quantity", "id")

result = (
    df_inventory
    .withColumn("quantity_avg_2", F.avg("quantity").over(w))
    .withColumn("quantity_percent_rank", F.percent_rank().over(w_ordered))
    .withColumn("next_quantity", F.lead("quantity", 1, 0).over(w3))
)
