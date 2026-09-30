from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")
w3 = Window.partitionBy("product_id", "store_id").orderBy("units", "id")

result = (
    df_store_sales
    .withColumn("units_avg_6", F.avg("units").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("prev_revenue", F.lag("revenue", 2).over(w3))
)
