from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .withColumn("running_sum_quantity", F.sum("quantity").over(w))
    .withColumn("running_min_quantity", F.min("quantity").over(w))
    .withColumn("running_avg_quantity", F.avg("quantity").over(w))
)
