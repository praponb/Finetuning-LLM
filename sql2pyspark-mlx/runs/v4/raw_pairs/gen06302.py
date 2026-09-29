from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id")

result = (
    df_store_sales
    .withColumn("running_revenue", F.sum("revenue").over(w))
    .withColumn("store_id_count_revenue", F.count("revenue").over(w2))
)
