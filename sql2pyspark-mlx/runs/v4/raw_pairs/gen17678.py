from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units", "id")
w2 = Window.partitionBy("product_id", "store_id")

result = (
    df_store_sales
    .withColumn("prev_revenue", F.lag("revenue", 2).over(w))
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w2)), 4))
)
